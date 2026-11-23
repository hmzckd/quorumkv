package transport

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hmzckd/quorumkv/internal/raft"
)

func TestSendPostsMessagesInOrder(t *testing.T) {
	got := make(chan raft.Message, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != MessagePath {
			t.Errorf("request %s %s", r.Method, r.URL.Path)
		}
		var m raft.Message
		json.NewDecoder(r.Body).Decode(&m)
		got <- m
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := NewHTTP(map[string]string{"n2": srv.URL}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tr.Start(ctx)

	tr.Send([]raft.Message{
		{Type: raft.MsgVote, From: "n1", To: "n2", Term: 4},
		{Type: raft.MsgApp, From: "n1", To: "n2", Term: 4, Entries: []raft.Entry{{Term: 4, Index: 1}}},
		{Type: raft.MsgVote, From: "n1", To: "n9", Term: 4},
	})

	for i, want := range []raft.MsgType{raft.MsgVote, raft.MsgApp} {
		select {
		case m := <-got:
			if m.Type != want || m.Term != 4 {
				t.Errorf("message %d = %+v, want %s", i, m, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("message %d never arrived", i)
		}
	}
}

func TestUnreachablePeerDoesNotBlockSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := NewHTTP(map[string]string{"n2": "http://127.0.0.1:1"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tr.Start(ctx)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 5000; i++ {
			tr.Send([]raft.Message{{Type: raft.MsgApp, From: "n1", To: "n2"}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on an unreachable peer")
	}
}
