package node_test

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLaggingFollowerCatchesUpFromSnapshot(t *testing.T) {
	c := startClusterOpts(t, 3, 0, 20)
	leader, _ := c.waitForLeader()
	lagging := without(c.ids, leader)[0]
	c.stop(lagging)

	for i := 0; i < 100; i++ {
		resp := request(t, http.DefaultClient, http.MethodPut, fmt.Sprintf("%s/kv/k%03d", c.urls[leader], i), fmt.Sprint(i))
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("PUT k%03d = %d", i, resp.StatusCode)
		}
	}
	if st := c.status(leader); st.SnapshotIndex < 80 {
		t.Fatalf("leader snapshot index = %d, want at least 80 after 100 writes with a snapshot every 20", st.SnapshotIndex)
	}
	info, err := os.Stat(filepath.Join(c.dirs[leader], "log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 4096 {
		t.Errorf("leader log is %d bytes, want it short after compaction", info.Size())
	}

	c.restart(lagging)
	c.eventually(3*time.Second, func() bool { return c.status(lagging).Keys == 100 })

	st := c.status(lagging)
	if st.Keys != 100 || st.SnapshotIndex == 0 {
		t.Fatalf("%s after restart = %+v, want 100 keys from an installed snapshot", lagging, st)
	}
}

func TestClusterRestartsFromSnapshots(t *testing.T) {
	c := startClusterOpts(t, 3, 0, 10)
	leader, _ := c.waitForLeader()
	for i := 0; i < 35; i++ {
		request(t, http.DefaultClient, http.MethodPut, fmt.Sprintf("%s/kv/k%02d", c.urls[leader], i), fmt.Sprint(i))
	}

	for _, id := range c.ids {
		c.stop(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	leader, _ = c.waitForLeader()

	code, body := get(t, http.DefaultClient, c.urls[leader]+"/kv/k07")
	if code != http.StatusOK || body != "7" {
		t.Fatalf("GET k07 after full restart = %d %q, want 200 7", code, body)
	}
	if st := c.status(leader); st.Keys != 35 {
		t.Fatalf("keys after restart = %d, want 35", st.Keys)
	}
}
