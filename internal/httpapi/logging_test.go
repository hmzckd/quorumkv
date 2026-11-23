package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEveryRequestIsLoggedOnce(t *testing.T) {
	var buf bytes.Buffer
	handler := NewHandler(slog.New(slog.NewJSONHandler(&buf, nil)), Options{NodeID: "n1"}, &fakeNode{})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log is not one JSON line: %v\n%s", err, buf.String())
	}
	if line["msg"] != "request" || line["path"] != "/nope" || line["status"] != float64(404) {
		t.Errorf("log line = %v", line)
	}
}
