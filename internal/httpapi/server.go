package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/hmzckd/quorumkv/internal/kv"
	"github.com/hmzckd/quorumkv/internal/node"
	"github.com/hmzckd/quorumkv/internal/raft"
	"github.com/hmzckd/quorumkv/internal/transport"
)

const defaultWriteTimeout = 2 * time.Second

type Node interface {
	Status(ctx context.Context) (node.Status, error)
	Deliver(m raft.Message) bool
	Propose(ctx context.Context, cmd kv.Command) error
}

type Options struct {
	NodeID       string
	PeerURLs     map[string]string
	WriteTimeout time.Duration
}

func NewHandler(logger *slog.Logger, opts Options, n Node) http.Handler {
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = defaultWriteTimeout
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health(opts.NodeID))
	mux.HandleFunc("GET /status", status(n))
	mux.HandleFunc("POST "+transport.MessagePath, receive(n))
	mux.HandleFunc("PUT /kv/{key}", put(n, opts))
	mux.HandleFunc("DELETE /kv/{key}", del(n, opts))
	return logRequests(logger, mux)
}
