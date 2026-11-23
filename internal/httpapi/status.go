package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/hmzckd/quorumkv/internal/raft"
)

const maxMessageBytes = 16 << 20

func status(node Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st, err := node.Status(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, st)
	}
}

func receive(node Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var m raft.Message
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxMessageBytes)).Decode(&m); err != nil {
			http.Error(w, "invalid message", http.StatusBadRequest)
			return
		}
		if !node.Deliver(m) {
			http.Error(w, "inbox full", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
