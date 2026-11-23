package sim

import (
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
