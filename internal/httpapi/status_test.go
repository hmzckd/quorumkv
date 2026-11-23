package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hmzckd/quorumkv/internal/raft"
)

func TestStatusShowsRaftState(t *testing.T) {
	node := &fakeNode{status: raft.Status{ID: "n2", Role: raft.Leader, Term: 4, Leader: "n2", LastIndex: 9, CommitIndex: 8}}
	rec := httptest.NewRecorder()

	NewHandler(quietLogger(), "n2", node).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got raft.Status
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got != node.status {
		t.Errorf("body = %+v, want %+v", got, node.status)
	}
}

func TestRaftMessageIsDelivered(t *testing.T) {
	node := &fakeNode{}
	body, _ := json.Marshal(raft.Message{Type: raft.MsgVote, From: "n1", To: "n2", Term: 3})
	rec := httptest.NewRecorder()

	NewHandler(quietLogger(), "n2", node).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/raft/message", bytes.NewReader(body)))

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if len(node.delivered) != 1 || node.delivered[0].Term != 3 {
		t.Fatalf("delivered = %+v", node.delivered)
	}
}

func TestRaftMessageErrors(t *testing.T) {
	cases := []struct {
		name string
		node *fakeNode
		body string
		want int
	}{
		{"bad json", &fakeNode{}, "{", http.StatusBadRequest},
		{"inbox full", &fakeNode{full: true}, `{"type":"RequestVote"}`, http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			NewHandler(quietLogger(), "n2", c.node).ServeHTTP(rec,
				httptest.NewRequest(http.MethodPost, "/raft/message", strings.NewReader(c.body)))
			if rec.Code != c.want {
				t.Errorf("status = %d, want %d", rec.Code, c.want)
			}
		})
	}
}
