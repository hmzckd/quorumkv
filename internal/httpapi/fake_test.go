package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"

	"github.com/hmzckd/quorumkv/internal/kv"
	"github.com/hmzckd/quorumkv/internal/node"
	"github.com/hmzckd/quorumkv/internal/raft"
)

type fakeNode struct {
	status    node.Status
	delivered []raft.Message
	full      bool
	proposed  []kv.Command
	err       error
	values    map[string]string
}

func (f *fakeNode) Get(_ context.Context, key string) (string, bool, error) {
	if f.err != nil {
		return "", false, f.err
	}
	v, ok := f.values[key]
	return v, ok, nil
}

func (f *fakeNode) Status(context.Context) (node.Status, error) { return f.status, nil }

func (f *fakeNode) Propose(_ context.Context, cmd kv.Command) error {
	f.proposed = append(f.proposed, cmd)
	return f.err
}

func handlerFor(f *fakeNode) http.Handler {
	return NewHandler(quietLogger(), Options{NodeID: "n2", PeerURLs: map[string]string{"n1": "http://n1:7001"}}, f)
}

func (f *fakeNode) Deliver(m raft.Message) bool {
	if f.full {
		return false
	}
	f.delivered = append(f.delivered, m)
	return true
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
