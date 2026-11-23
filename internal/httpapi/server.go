package httpapi

import (
	"log/slog"
	"net/http"
)

func NewHandler(logger *slog.Logger, nodeID string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health(nodeID))
	return logRequests(logger, mux)
}
