package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthReportsNode(t *testing.T) {
	srv := httptest.NewServer(NewHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), "n2"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body != (healthResponse{Status: "ok", Node: "n2"}) {
		t.Errorf("body = %+v, want ok/n2", body)
	}
}

func TestHealthRejectsPost(t *testing.T) {
	srv := httptest.NewServer(NewHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), "n1"))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/health", "text/plain", nil)
	if err != nil {
		t.Fatalf("POST /health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}
