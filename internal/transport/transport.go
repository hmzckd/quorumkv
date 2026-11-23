package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/hmzckd/quorumkv/internal/raft"
)

const (
	MessagePath = "/raft/message"
	queueSize   = 1024
	sendTimeout = 300 * time.Millisecond
)

type HTTP struct {
	queues map[string]chan raft.Message
	urls   map[string]string
	client *http.Client
	logger *slog.Logger
}

func NewHTTP(peerURLs map[string]string, logger *slog.Logger) *HTTP {
	t := &HTTP{
		queues: map[string]chan raft.Message{},
		urls:   peerURLs,
		client: &http.Client{Timeout: sendTimeout},
		logger: logger,
	}
	for id := range peerURLs {
		t.queues[id] = make(chan raft.Message, queueSize)
	}
	return t
}

func (t *HTTP) Start(ctx context.Context) {
	for id, q := range t.queues {
		go t.sendLoop(ctx, id, q)
	}
}

func (t *HTTP) Send(msgs []raft.Message) {
	for _, m := range msgs {
		q, ok := t.queues[m.To]
		if !ok {
			continue
		}
		select {
		case q <- m:
		default:
			t.logger.Debug("dropped message, queue full", "to", m.To, "type", m.Type)
		}
	}
}

func (t *HTTP) sendLoop(ctx context.Context, peer string, q chan raft.Message) {
	url := t.urls[peer] + MessagePath
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-q:
			if err := t.post(ctx, url, m); err != nil {
				t.logger.Debug("send failed", "to", peer, "type", m.Type, "error", err)
			}
		}
	}
}

func (t *HTTP) post(ctx context.Context, url string, m raft.Message) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
