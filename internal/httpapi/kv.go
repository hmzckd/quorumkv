package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/hmzckd/quorumkv/internal/kv"
	"github.com/hmzckd/quorumkv/internal/node"
)

const (
	maxKeyBytes   = 256
	maxValueBytes = 64 << 10
)

func put(n Node, opts Options) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, ok := validKey(w, r)
		if !ok {
			return
		}
		value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
		if err != nil {
			writeProblem(w, http.StatusRequestEntityTooLarge, "Value too large", "values are limited to 64 KiB")
			return
		}
		write(w, r, n, opts, kv.Command{Op: kv.OpPut, Key: key, Value: string(value)})
	}
}

func del(n Node, opts Options) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, ok := validKey(w, r)
		if !ok {
			return
		}
		write(w, r, n, opts, kv.Command{Op: kv.OpDelete, Key: key})
	}
}

func write(w http.ResponseWriter, r *http.Request, n Node, opts Options, cmd kv.Command) {
	ctx, cancel := context.WithTimeout(r.Context(), opts.WriteTimeout)
	defer cancel()

	err := n.Propose(ctx, cmd)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeError(w, r, opts, err)
}

func writeError(w http.ResponseWriter, r *http.Request, opts Options, err error) {
	var notLeader *node.NotLeaderError
	switch {
	case errors.As(err, &notLeader) && notLeader.Leader != "":
		target, ok := opts.PeerURLs[notLeader.Leader]
		if !ok {
			writeProblem(w, http.StatusServiceUnavailable, "Leader unreachable", err.Error())
			return
		}
		w.Header().Set("Location", target+r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
	case errors.As(err, &notLeader):
		w.Header().Set("Retry-After", "1")
		writeProblem(w, http.StatusServiceUnavailable, "No leader", "an election is in progress, retry in a moment")
	case errors.Is(err, node.ErrUnknownOutcome):
		writeProblem(w, http.StatusServiceUnavailable, "Outcome unknown", err.Error())
	default:
		writeProblem(w, http.StatusServiceUnavailable, "Not applied", err.Error())
	}
}

func validKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.PathValue("key")
	if key == "" || len(key) > maxKeyBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid key", "keys must be 1 to 256 bytes")
		return "", false
	}
	return key, true
}

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

func writeProblem(w http.ResponseWriter, code int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	writeBody(w, problem{Type: "about:blank", Title: title, Status: code, Detail: detail})
}
