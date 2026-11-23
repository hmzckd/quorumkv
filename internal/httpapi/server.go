package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/hmzckd/quorumkv/internal/raft"
	"github.com/hmzckd/quorumkv/internal/transport"
)

type Node interface {
	Status(ctx context.Context) (raft.Status, error)
	Deliver(m raft.Message) bool
}

func NewHandler(logger *slog.Logger, nodeID string, node Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health(nodeID))
	mux.HandleFunc("GET /status", status(node))
	mux.HandleFunc("POST "+transport.MessagePath, receive(node))
	return logRequests(logger, mux)
}
