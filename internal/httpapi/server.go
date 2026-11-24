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

const defaultTimeout = 2 * time.Second

type Node interface {
	Status(ctx context.Context) (node.Status, error)
	Deliver(m raft.Message) bool
	Propose(ctx context.Context, cmd kv.Command) error
	Get(ctx context.Context, key string) (string, bool, error)
}

type Options struct {
	NodeID   string
	PeerURLs map[string]string
	Timeout  time.Duration
}

func NewHandler(logger *slog.Logger, opts Options, n Node) http.Handler {
	if opts.Timeout == 0 {
		opts.Timeout = defaultTimeout
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health(opts.NodeID))
	mux.HandleFunc("GET /status", status(n))
	mux.HandleFunc("POST "+transport.MessagePath, receive(n))
	mux.HandleFunc("GET /kv/{key}", get(n, opts))
	mux.HandleFunc("PUT /kv/{key}", put(n, opts))
	mux.HandleFunc("DELETE /kv/{key}", del(n, opts))
	return logRequests(logger, mux)
}
