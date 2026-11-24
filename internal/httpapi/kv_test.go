package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hmzckd/quorumkv/internal/kv"
	"github.com/hmzckd/quorumkv/internal/node"
)

func do(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, strings.NewReader(body)))
	return rec
}

func TestPutAndDeleteProposeCommands(t *testing.T) {
	fake := &fakeNode{}
	h := handlerFor(fake)

	if rec := do(h, http.MethodPut, "/kv/color", "blue"); rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d", rec.Code)
	}
	if rec := do(h, http.MethodDelete, "/kv/color", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", rec.Code)
	}

	want := []kv.Command{{Op: kv.OpPut, Key: "color", Value: "blue"}, {Op: kv.OpDelete, Key: "color"}}
	if len(fake.proposed) != 2 || fake.proposed[0] != want[0] || fake.proposed[1] != want[1] {
		t.Fatalf("proposed = %+v, want %+v", fake.proposed, want)
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	rec := do(handlerFor(&fakeNode{err: &node.NotLeaderError{Leader: "n1"}}), http.MethodPut, "/kv/color?x=1", "blue")

	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "http://n1:7001/kv/color?x=1" {
		t.Errorf("Location = %q", loc)
	}
}

func TestWriteErrorsAre503(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		retryAfter string
		title      string
	}{
		{"no leader", &node.NotLeaderError{}, "1", "No leader"},
		{"unknown leader address", &node.NotLeaderError{Leader: "n7"}, "", "Leader unreachable"},
		{"outcome unknown", node.ErrUnknownOutcome, "", "Outcome unknown"},
		{"overwritten", node.ErrNotCommitted, "", "Not applied"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := do(handlerFor(&fakeNode{err: c.err}), http.MethodPut, "/kv/x", "1")
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", rec.Code)
			}
			if got := rec.Header().Get("Retry-After"); got != c.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, c.retryAfter)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("Content-Type = %q", ct)
			}
			if !strings.Contains(rec.Body.String(), c.title) {
				t.Errorf("body = %s, want title %q", rec.Body.String(), c.title)
			}
		})
	}
}

func TestBadKeysAndValues(t *testing.T) {
	h := handlerFor(&fakeNode{})
	if rec := do(h, http.MethodPut, "/kv/"+strings.Repeat("k", 257), "1"); rec.Code != http.StatusBadRequest {
		t.Errorf("long key status = %d, want 400", rec.Code)
	}
	if rec := do(h, http.MethodPut, "/kv/x", strings.Repeat("v", 64<<10+1)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("large value status = %d, want 413", rec.Code)
	}
}

func TestGetReturnsValueOr404(t *testing.T) {
	h := handlerFor(&fakeNode{values: map[string]string{"color": "blue"}})

	rec := do(h, http.MethodGet, "/kv/color", "")
	if rec.Code != http.StatusOK || rec.Body.String() != "blue" {
		t.Fatalf("GET color = %d %q, want 200 blue", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodGet, "/kv/size", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET size = %d, want 404", rec.Code)
	}
}

func TestGetOnFollowerRedirects(t *testing.T) {
	rec := do(handlerFor(&fakeNode{err: &node.NotLeaderError{Leader: "n1"}}), http.MethodGet, "/kv/color", "")
	if rec.Code != http.StatusTemporaryRedirect || rec.Header().Get("Location") != "http://n1:7001/kv/color" {
		t.Fatalf("GET on follower = %d %q", rec.Code, rec.Header().Get("Location"))
	}
}

func TestGetErrorsAre503(t *testing.T) {
	for _, err := range []error{node.ErrNotReady, node.ErrTimeout} {
		rec := do(handlerFor(&fakeNode{err: err}), http.MethodGet, "/kv/color", "")
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%v: status = %d, want 503", err, rec.Code)
		}
	}
}
