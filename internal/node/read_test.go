package node_test

import (
	"io"
	"net/http"
	"testing"
	"time"
)

func get(t *testing.T, client *http.Client, url string) (int, string) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestReadYourWriteFromAnyNode(t *testing.T) {
	c := startCluster(t, 3)
	leader, _ := c.waitForLeader()
	c.eventually(time.Second, func() bool {
		code, _ := get(t, http.DefaultClient, c.urls[leader]+"/kv/none")
		return code == http.StatusNotFound
	})

	if resp := request(t, http.DefaultClient, http.MethodPut, c.urls[leader]+"/kv/color", "blue"); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT = %d", resp.StatusCode)
	}

	for _, id := range c.ids {
		code, body := get(t, http.DefaultClient, c.urls[id]+"/kv/color")
		if code != http.StatusOK || body != "blue" {
			t.Errorf("GET via %s = %d %q, want 200 blue right after the write", id, code, body)
		}
	}
	if code, _ := get(t, http.DefaultClient, c.urls[leader]+"/kv/missing"); code != http.StatusNotFound {
		t.Errorf("missing key = %d, want 404", code)
	}
}

func TestFollowerRedirectsReads(t *testing.T) {
	c := startCluster(t, 3)
	leader, _ := c.waitForLeader()
	follower := without(c.ids, leader)[0]

	resp, err := noRedirect.Get(c.urls[follower] + "/kv/color")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect || resp.Header.Get("Location") != c.urls[leader]+"/kv/color" {
		t.Fatalf("GET on follower = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestLeaderWithoutMajorityRefusesReads(t *testing.T) {
	c := startClusterWith(t, 3, 500*time.Millisecond)
	leader, _ := c.waitForLeader()
	request(t, http.DefaultClient, http.MethodPut, c.urls[leader]+"/kv/color", "blue")
	for _, id := range without(c.ids, leader) {
		c.stop(id)
	}

	code, body := get(t, http.DefaultClient, c.urls[leader]+"/kv/color")

	if code != http.StatusServiceUnavailable {
		t.Fatalf("GET on a leader without majority = %d %q, want 503: it cannot prove it is still leader", code, body)
	}
}
