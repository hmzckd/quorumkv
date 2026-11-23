package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hmzckd/quorumkv/internal/config"
	"github.com/hmzckd/quorumkv/internal/httpapi"
	"github.com/hmzckd/quorumkv/internal/node"
	"github.com/hmzckd/quorumkv/internal/transport"
)

const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("quorumkv stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("node", cfg.NodeID)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	peerURLs := map[string]string{}
	for _, p := range cfg.Peers {
		if p.ID != cfg.NodeID {
			peerURLs[p.ID] = p.URL
		}
	}
	tr := transport.NewHTTP(peerURLs, logger)

	n, err := node.New(node.Config{
		ID:             cfg.NodeID,
		Peers:          cfg.PeerIDs(),
		DataDir:        cfg.DataDir,
		Tick:           config.Tick,
		ElectionTicks:  cfg.ElectionTicks(),
		HeartbeatTicks: cfg.HeartbeatTicks(),
	}, tr, logger)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}

	srv := &http.Server{
		Handler:           httpapi.NewHandler(logger, httpapi.Options{NodeID: cfg.NodeID, PeerURLs: peerURLs}, n),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	logger.Info("cluster config", "peers", cfg.PeerIDs(), "quorum", cfg.Quorum(), "data_dir", cfg.DataDir,
		"heartbeat", cfg.Heartbeat.String(), "election_timeout", cfg.ElectionTimeout.String())

	tr.Start(ctx)
	nodeErr := make(chan error, 1)
	go func() {
		err := n.Run(ctx)
		if err != nil {
			logger.Error("raft loop failed, stopping", "error", err)
			cancel()
		}
		nodeErr <- err
	}()

	if err := serve(ctx, logger, srv, listener); err != nil {
		return err
	}
	return <-nodeErr
}

func serve(ctx context.Context, logger *slog.Logger, srv *http.Server, listener net.Listener) error {
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("quorumkv started", "addr", listener.Addr().String())
		serveErr <- srv.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutting down", "timeout", shutdownTimeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	logger.Info("quorumkv stopped cleanly")
	return nil
}
