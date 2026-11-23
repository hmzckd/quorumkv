package httpapi

import (
	"context"
	"io"
	"log/slog"

	"github.com/hmzckd/quorumkv/internal/raft"
)

type fakeNode struct {
	status    raft.Status
	delivered []raft.Message
	full      bool
}

func (f *fakeNode) Status(context.Context) (raft.Status, error) { return f.status, nil }

func (f *fakeNode) Deliver(m raft.Message) bool {
	if f.full {
		return false
	}
	f.delivered = append(f.delivered, m)
	return true
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
