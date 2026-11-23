package node_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func request(t *testing.T, client *http.Client, method, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

var noRedirect = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func TestWriteOnLeaderReachesEveryNode(t *testing.T) {
	c := startCluster(t, 3)
	leader, _ := c.waitForLeader()

	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if resp := request(t, http.DefaultClient, http.MethodPut, c.urls[leader]+"/kv/"+kv[0], kv[1]); resp.StatusCode != http.StatusNoContent {
			t.Fatalf("PUT %s = %d", kv[0], resp.StatusCode)
		}
	}
	if resp := request(t, http.DefaultClient, http.MethodDelete, c.urls[leader]+"/kv/b", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE = %d", resp.StatusCode)
	}

	c.eventually(2*time.Second, func() bool {
		for _, id := range c.ids {
			if c.status(id).Keys != 2 {
				return false
			}
		}
		return true
	})
	for _, id := range c.ids {
		if keys := c.status(id).Keys; keys != 2 {
			t.Errorf("%s has %d keys, want 2 (a and c)", id, keys)
		}
	}
}

func TestFollowerRedirectsWritesToLeader(t *testing.T) {
	c := startCluster(t, 3)
	leader, _ := c.waitForLeader()
	follower := without(c.ids, leader)[0]

	resp := request(t, noRedirect, http.MethodPut, c.urls[follower]+"/kv/x", "1")
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != c.urls[leader]+"/kv/x" {
		t.Fatalf("Location = %q, want %s/kv/x", loc, c.urls[leader])
	}

	if resp := request(t, http.DefaultClient, http.MethodPut, c.urls[follower]+"/kv/x", "1"); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT following the redirect = %d, want 204", resp.StatusCode)
	}
}

func TestLeaderWithoutMajorityAnswersOutcomeUnknown(t *testing.T) {
	c := startClusterWith(t, 3, 500*time.Millisecond)
	leader, _ := c.waitForLeader()
	for _, id := range without(c.ids, leader) {
		c.stop(id)
	}

	started := time.Now()
	resp := request(t, http.DefaultClient, http.MethodPut, c.urls[leader]+"/kv/x", "1")

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if took := time.Since(started); took < 400*time.Millisecond {
		t.Errorf("answered after %s, want it to wait for the write timeout", took)
	}
	if st := c.status(leader); st.Keys != 0 {
		t.Errorf("leader applied the write without a majority: %d keys", st.Keys)
	}
}
