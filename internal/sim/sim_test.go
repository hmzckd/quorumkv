package sim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hmzckd/quorumkv/internal/raft"
)

func mustLeader(t *testing.T, c *Cluster, ids ...string) string {
	t.Helper()
	if len(ids) == 0 {
		ids = c.IDs()
	}
	if err := c.RunUntil(1000, c.HasLeaderIn(ids...)); err != nil {
		t.Fatalf("no leader among %v: %v\n%s", ids, err, c.Timeline())
	}
	for _, id := range ids {
		if c.Status(id).Role == raft.Leader && c.IsUp(id) {
			return id
		}
	}
	panic("unreachable")
}

func others(c *Cluster, id string) []string {
	var out []string
	for _, x := range c.IDs() {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

func TestClusterElectsOneLeader(t *testing.T) {
	c := New(Options{Seed: 1})
	mustLeader(t, c)
	if err := c.Run(500); err != nil {
		t.Fatal(err)
	}
	if got := c.Leaders(); len(got) != 1 {
		t.Fatalf("leaders = %v, want exactly one", got)
	}
}

func TestSameSeedGivesSameTimeline(t *testing.T) {
	run := func() string {
		c := New(Options{Seed: 42, DropRate: 0.2, MaxDelay: 8})
		if err := c.Run(2000); err != nil {
			t.Fatal(err)
		}
		return strings.Join(c.Events(), "\n")
	}
	a, b := run(), run()
	if a != b {
		t.Fatalf("two runs with seed 42 differ:\n--- first\n%s\n--- second\n%s", a, b)
	}
	if a == "" {
		t.Fatal("timeline is empty")
	}
}

func TestDifferentSeedsGiveDifferentTimelines(t *testing.T) {
	timeline := func(seed uint64) string {
		c := New(Options{Seed: seed, DropRate: 0.2, MaxDelay: 8})
		_ = c.Run(2000)
		return strings.Join(c.Events(), "\n")
	}
	if timeline(1) == timeline(2) {
		t.Fatal("seeds 1 and 2 produced the same timeline")
	}
}

func TestElectsLeaderDespiteMessageLoss(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		c := New(Options{Seed: seed, DropRate: 0.3, MaxDelay: 5})
		mustLeader(t, c)
		if err := c.Run(1000); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCrashedLeaderIsReplaced(t *testing.T) {
	c := New(Options{Seed: 3})
	old := mustLeader(t, c)
	oldTerm := c.Status(old).Term

	c.Crash(old)
	next := mustLeader(t, c, others(c, old)...)

	if term := c.Status(next).Term; term <= oldTerm {
		t.Fatalf("new leader term %d, want > %d", term, oldTerm)
	}
}

func TestPartitionedLeaderStillThinksItLeads(t *testing.T) {
	c := New(Options{Seed: 7})
	old := mustLeader(t, c)
	oldTerm := c.Status(old).Term
	majority := others(c, old)

	c.Partition([]string{old}, majority)
	next := mustLeader(t, c, majority...)
	if err := c.Run(200); err != nil {
		t.Fatal(err)
	}

	if st := c.Status(old); st.Role != raft.Leader || st.Term != oldTerm {
		t.Errorf("old leader %s = %+v, want still leader of term %d: nobody told it", old, st, oldTerm)
	}
	if term := c.Status(next).Term; term <= oldTerm {
		t.Errorf("new leader %s has term %d, want > %d", next, term, oldTerm)
	}

	c.Heal()
	if err := c.Run(200); err != nil {
		t.Fatal(err)
	}
	if got := c.Leaders(); len(got) != 1 {
		t.Errorf("after heal leaders = %v, want one", got)
	}
	if st := c.Status(old); st.Role != raft.Follower {
		t.Errorf("old leader %s after heal = %+v, want follower", old, st)
	}

	t.Logf("timeline:\n%s", c.Timeline())
}

func TestIsolatedNodesNeverElectALeader(t *testing.T) {
	c := New(Options{Seed: 5})
	c.Partition([]string{"n1"}, []string{"n2"}, []string{"n3"})

	if err := c.Run(2000); err != nil {
		t.Fatal(err)
	}
	if got := c.Leaders(); len(got) != 0 {
		t.Fatalf("leaders = %v, want none: one node is not a majority of three", got)
	}
	if term := c.Status("n1").Term; term < 10 {
		t.Errorf("n1 term = %d, expected many failed elections", term)
	}
}

func TestFiveNodesSurviveTwoCrashesButNotThree(t *testing.T) {
	c := New(Options{Seed: 11, Nodes: 5})
	first := mustLeader(t, c)

	c.Crash(first)
	rest := others(c, first)
	second := mustLeader(t, c, rest...)
	c.Crash(second)

	var alive []string
	for _, id := range rest {
		if id != second {
			alive = append(alive, id)
		}
	}
	third := mustLeader(t, c, alive...)
	c.Crash(third)

	if err := c.Run(2000); err != nil {
		t.Fatal(err)
	}
	if got := c.Leaders(); len(got) != 0 {
		t.Fatalf("leaders = %v with only 2 of 5 nodes up, want none", got)
	}
}

func TestRestartedNodeRemembersItsTerm(t *testing.T) {
	c := New(Options{Seed: 9})
	leader := mustLeader(t, c)
	follower := others(c, leader)[0]
	term := c.Status(follower).Term

	c.Crash(follower)
	c.Restart(follower)

	if got := c.Status(follower).Term; got != term {
		t.Fatalf("restarted %s has term %d, want %d from disk", follower, got, term)
	}
}

func TestElectionSafetyCheckCatchesTwoLeaders(t *testing.T) {
	c := New(Options{Seed: 1})
	c.nodes["n1"].status = raft.Status{ID: "n1", Role: raft.Leader, Term: 4}
	c.nodes["n2"].status = raft.Status{ID: "n2", Role: raft.Leader, Term: 4}

	err := c.checkElectionSafety()
	if err == nil || !strings.Contains(err.Error(), "term 4") {
		t.Fatalf("err = %v, want an election safety violation for term 4", err)
	}
}

func TestMinorityWritesAreDiscarded(t *testing.T) {
	c := New(Options{Seed: 7})
	old := mustLeader(t, c)
	majority := others(c, old)

	c.Partition([]string{old}, majority)
	lostIndex, _, err := c.Propose(old, "x=lost")
	if err != nil {
		t.Fatalf("propose on old leader: %v", err)
	}
	next := mustLeader(t, c, majority...)
	keptIndex, _, err := c.Propose(next, "x=kept")
	if err != nil {
		t.Fatalf("propose on new leader: %v", err)
	}
	if err := c.Run(100); err != nil {
		t.Fatal(err)
	}
	if e, ok := c.Committed(lostIndex); ok && string(e.Data) == "x=lost" {
		t.Fatalf("minority write was committed: %+v", e)
	}

	c.Heal()
	if err := c.Run(200); err != nil {
		t.Fatal(err)
	}

	for _, e := range c.Log(old) {
		if string(e.Data) == "x=lost" {
			t.Fatalf("%s still has x=lost after healing", old)
		}
	}
	if e, ok := c.Committed(keptIndex); !ok || string(e.Data) != "x=kept" {
		t.Fatalf("x=kept not committed at %d", keptIndex)
	}
	if got := c.Status(old).CommitIndex; got < keptIndex {
		t.Errorf("%s commit = %d, want at least %d", old, got, keptIndex)
	}

	t.Logf("timeline:\n%s", c.Timeline())
}

func TestCommittedWriteSurvivesLeaderCrash(t *testing.T) {
	c := New(Options{Seed: 13})
	leader := mustLeader(t, c)
	index, _, _ := c.Propose(leader, "x=1")
	if err := c.RunUntil(200, func() bool { _, ok := c.Committed(index); return ok }); err != nil {
		t.Fatal(err)
	}

	c.Crash(leader)
	next := mustLeader(t, c, others(c, leader)...)
	if err := c.Run(50); err != nil {
		t.Fatal(err)
	}

	found := false
	for _, e := range c.Log(next) {
		found = found || string(e.Data) == "x=1"
	}
	if !found {
		t.Fatalf("new leader %s lost the committed write", next)
	}
}

func TestReplicationUnderMessageLoss(t *testing.T) {
	for seed := uint64(1); seed <= 30; seed++ {
		c := New(Options{Seed: seed, DropRate: 0.2, MaxDelay: 6})
		accepted := 0
		for round := 0; round < 100; round++ {
			if leader, _ := c.LeaderOfHighestTerm(); leader != "" {
				if _, _, err := c.Propose(leader, fmt.Sprintf("v%d", round)); err == nil {
					accepted++
				}
			}
			if err := c.Run(10); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Run(300); err != nil {
			t.Fatal(err)
		}
		if len(c.committed) < accepted/2 {
			t.Errorf("seed %d: only %d of %d accepted proposals committed", seed, len(c.committed), accepted)
		}
	}
}

func TestPartitionedLeaderCannotConfirmReads(t *testing.T) {
	c := New(Options{Seed: 7})
	old := mustLeader(t, c)
	index, _, _ := c.Propose(old, "x=1")
	if err := c.RunUntil(200, func() bool { _, ok := c.Committed(index); return ok }); err != nil {
		t.Fatal(err)
	}
	majority := others(c, old)

	c.Partition([]string{old}, majority)
	staleRead, err := c.ReadIndex(old)
	if err != nil {
		t.Fatalf("ReadIndex on the old leader: %v", err)
	}
	next := mustLeader(t, c, majority...)
	c.Propose(next, "x=2")

	var freshRead uint64
	if err := c.RunUntil(200, func() bool {
		if freshRead == 0 {
			freshRead, _ = c.ReadIndex(next)
		}
		return freshRead != 0
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Run(300); err != nil {
		t.Fatal(err)
	}

	if _, ok := c.ReadConfirmed(old, staleRead); ok {
		t.Fatalf("old leader %s confirmed a read while cut off: it would have returned x=1 after x=2", old)
	}
	if _, ok := c.ReadConfirmed(next, freshRead); !ok {
		t.Fatalf("new leader %s never confirmed its read", next)
	}
	t.Logf("timeline:\n%s", c.Timeline())
}

func TestCrashedFollowerCatchesUpThroughSnapshot(t *testing.T) {
	c := New(Options{Seed: 21, SnapshotEvery: 10})
	leader := mustLeader(t, c)
	lagging := others(c, leader)[0]
	c.Crash(lagging)

	var last uint64
	for i := 0; i < 50; i++ {
		last, _, _ = c.Propose(leader, fmt.Sprintf("v%d", i))
		if err := c.Run(2); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.RunUntil(200, func() bool { return c.Status(leader).CommitIndex >= last }); err != nil {
		t.Fatal(err)
	}

	c.Restart(lagging)
	if err := c.RunUntil(500, func() bool { return c.nodes[lagging].applied >= last }); err != nil {
		t.Fatal(err)
	}
	if err := c.Run(50); err != nil {
		t.Fatal(err)
	}

	installed := false
	for _, e := range c.Events() {
		installed = installed || strings.Contains(e, lagging+" installed snapshot")
	}
	if !installed {
		t.Fatalf("%s caught up without a snapshot:\n%s", lagging, c.Timeline())
	}
	if c.nodes[lagging].digest != c.nodes[leader].digest {
		t.Fatal("follower state differs from leader after the snapshot")
	}
}

func TestSnapshotsUnderMessageLossAndCrashes(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		c := New(Options{Seed: seed, SnapshotEvery: 7, DropRate: 0.1, MaxDelay: 5})
		for round := 0; round < 60; round++ {
			if leader, _ := c.LeaderOfHighestTerm(); leader != "" {
				c.Propose(leader, fmt.Sprintf("v%d", round))
			}
			if round%15 == 7 {
				victim := c.IDs()[round%3]
				c.Crash(victim)
				if err := c.Run(40); err != nil {
					t.Fatal(err)
				}
				c.Restart(victim)
			}
			if err := c.Run(10); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Run(300); err != nil {
			t.Fatal(err)
		}
	}
}
