package node_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/hmzckd/quorumkv/internal/httpapi"
	"github.com/hmzckd/quorumkv/internal/node"
	"github.com/hmzckd/quorumkv/internal/raft"
	"github.com/hmzckd/quorumkv/internal/transport"
)

type testCluster struct {
	t     *testing.T
	ids   []string
	urls  map[string]string
	addrs map[string]string
	dirs  map[string]string
	procs map[string]*process

	writeTimeout time.Duration
}

type process struct {
	node   *node.Node
	srv    *http.Server
	cancel context.CancelFunc
	done   chan error
}

func startCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	return startClusterWith(t, size, 0)
}

func startClusterWith(t *testing.T, size int, writeTimeout time.Duration) *testCluster {
	t.Helper()
	c := &testCluster{t: t, urls: map[string]string{}, addrs: map[string]string{}, dirs: map[string]string{},
		procs: map[string]*process{}, writeTimeout: writeTimeout}
	listeners := map[string]net.Listener{}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("n%d", i)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.ids = append(c.ids, id)
		listeners[id] = l
		c.addrs[id] = l.Addr().String()
		c.urls[id] = "http://" + l.Addr().String()
		c.dirs[id] = filepath.Join(t.TempDir(), id)
	}
	for _, id := range c.ids {
		c.start(id, listeners[id])
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) start(id string, l net.Listener) {
	c.t.Helper()
	if l == nil {
		var err error
		if l, err = net.Listen("tcp", c.addrs[id]); err != nil {
			c.t.Fatalf("listen again on %s: %v", c.addrs[id], err)
		}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	peers := map[string]string{}
	for _, p := range c.ids {
		if p != id {
			peers[p] = c.urls[p]
		}
	}
	tr := transport.NewHTTP(peers, logger)
	n, err := node.New(node.Config{
		ID: id, Peers: c.ids, DataDir: c.dirs[id],
		Tick: 10 * time.Millisecond, ElectionTicks: 15, HeartbeatTicks: 5,
	}, tr, logger)
	if err != nil {
		c.t.Fatalf("node.New(%s): %v", id, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &process{node: n, cancel: cancel, done: make(chan error, 1),
		srv: &http.Server{Handler: httpapi.NewHandler(logger, httpapi.Options{NodeID: id, PeerURLs: c.urls, Timeout: c.writeTimeout}, n)}}
	tr.Start(ctx)
	go func() { p.done <- n.Run(ctx) }()
	go p.srv.Serve(l)
	c.procs[id] = p
}

func (c *testCluster) stop(id string) {
	p := c.procs[id]
	if p == nil {
		return
	}
	p.cancel()
	p.srv.Close()
	if err := <-p.done; err != nil {
		c.t.Errorf("%s stopped with error: %v", id, err)
	}
	delete(c.procs, id)
}

func (c *testCluster) restart(id string) {
	c.start(id, nil)
}

func (c *testCluster) status(id string) node.Status {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	st, err := c.procs[id].node.Status(ctx)
	if err != nil {
		c.t.Fatalf("status %s: %v", id, err)
	}
	return st
}

func (c *testCluster) waitForLeader(among ...string) (string, uint64) {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []node.Status
		agree := true
		for _, id := range among {
			st := c.status(id)
			if st.Role == raft.Leader {
				leaders = append(leaders, st)
			}
			agree = agree && st.Leader != ""
		}
		if len(leaders) == 1 && agree {
			return leaders[0].ID, leaders[0].Term
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single agreed leader among %v within 5s", among)
	return "", 0
}

func (c *testCluster) eventually(limit time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func without(ids []string, id string) []string {
	var out []string
	for _, x := range ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

func TestThreeNodesOverHTTPElectOneLeader(t *testing.T) {
	c := startCluster(t, 3)
	leader, term := c.waitForLeader()
	c.eventually(time.Second, func() bool {
		for _, id := range c.ids {
			if c.status(id).CommitIndex < 1 {
				return false
			}
		}
		return true
	})

	for _, id := range c.ids {
		st := c.status(id)
		if st.Leader != leader || st.Term != term {
			t.Errorf("%s sees leader %q term %d, want %s term %d", id, st.Leader, st.Term, leader, term)
		}
		if st.CommitIndex < 1 {
			t.Errorf("%s commit = %d, want the leader's no-op committed", id, st.CommitIndex)
		}
	}
}

func TestLeaderFailoverOverHTTP(t *testing.T) {
	c := startCluster(t, 3)
	old, oldTerm := c.waitForLeader()

	c.stop(old)
	started := time.Now()
	next, term := c.waitForLeader(without(c.ids, old)...)
	t.Logf("%s stopped, %s became leader of term %d after %s", old, next, term, time.Since(started).Round(10*time.Millisecond))

	if next == old || term <= oldTerm {
		t.Fatalf("new leader %s term %d, want another node with term > %d", next, term, oldTerm)
	}

	c.restart(old)
	if leader, _ := c.waitForLeader(); leader != next {
		t.Logf("leadership moved to %s after %s came back", leader, old)
	}
	if st := c.status(old); st.Role != raft.Follower && st.Role != raft.Leader {
		t.Errorf("%s after restart = %+v", old, st)
	}
	if st := c.status(old); st.Term < term {
		t.Errorf("%s term = %d after rejoining, want at least %d", old, st.Term, term)
	}
}
