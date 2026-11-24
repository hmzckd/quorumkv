package raft

import (
	"math/rand/v2"
	"testing"
)

func TestCompactDropsAppliedEntries(t *testing.T) {
	net := committedLeader(t)
	leader := net.nodes["n1"]
	for i := 0; i < 5; i++ {
		leader.Propose([]byte{byte('a' + i)})
		net.deliver()
	}
	leader.Ready()

	snap, err := leader.Compact(4, []byte("state@4"))
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if snap.Index != 4 || snap.Term != 1 {
		t.Errorf("snapshot = %+v, want index 4 term 1", snap)
	}
	st := leader.Status()
	if st.SnapshotIndex != 4 || st.LastIndex != 6 {
		t.Errorf("status = %+v, want snapshot 4 and last 6", st)
	}
	if got := leader.entriesFrom(5, 0); len(got) != 2 || got[0].Index != 5 {
		t.Errorf("entries after compaction = %+v", got)
	}
	if _, err := leader.Compact(9, nil); err == nil {
		t.Error("compacting beyond the applied index must fail")
	}
}

func TestLeaderSendsSnapshotToFollowerThatIsTooFarBehind(t *testing.T) {
	net := committedLeader(t)
	leader := net.nodes["n1"]
	net.down["n3"] = true
	for i := 0; i < 20; i++ {
		leader.Propose([]byte{byte(i)})
		net.deliver()
	}
	leader.Ready()
	if _, err := leader.Compact(15, []byte("state@15")); err != nil {
		t.Fatal(err)
	}

	net.down["n3"] = false
	sawSnapshot := false
	for i := 0; i < 10 && !sawSnapshot; i++ {
		leader.Tick()
		for _, m := range leader.Ready().Messages {
			sawSnapshot = sawSnapshot || (m.To == "n3" && m.Type == MsgSnap)
			net.nodes[m.To].Step(m)
		}
		net.deliver()
	}
	if !sawSnapshot {
		t.Fatal("leader never sent InstallSnapshot to n3")
	}
	for i := 0; i < 10; i++ {
		net.tickAll()
	}

	st := net.nodes["n3"].Status()
	if st.SnapshotIndex != 15 || st.LastIndex != 21 || st.CommitIndex != 21 {
		t.Fatalf("n3 status = %+v, want snapshot 15, last and commit 21", st)
	}
}

func TestInstalledSnapshotComesOutOfReady(t *testing.T) {
	r := newTestRaft(t, "n2", []string{"n1", "n2", "n3"}, 1)
	snap := &Snapshot{Index: 10, Term: 3, Data: []byte("state@10")}

	r.Step(Message{Type: MsgSnap, From: "n1", To: "n2", Term: 3, Snapshot: snap})
	rd := r.Ready()

	if rd.Snapshot == nil || rd.Snapshot.Index != 10 {
		t.Fatalf("ready snapshot = %+v", rd.Snapshot)
	}
	if len(rd.Committed) != 0 {
		t.Errorf("committed = %+v, want none: the snapshot already holds them", rd.Committed)
	}
	if m := rd.Messages[0]; !m.Success || m.MatchIndex != 10 {
		t.Errorf("reply = %+v, want success with match 10", m)
	}
}

func TestSnapshotKeepsMatchingEntriesAfterIt(t *testing.T) {
	r, err := New(Config{ID: "n2", Peers: []string{"n1", "n2", "n3"}, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: rand.New(rand.NewPCG(1, 1))}, State{
		HardState: HardState{Term: 2},
		Entries:   []Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}, {Term: 2, Index: 4}},
	})
	if err != nil {
		t.Fatal(err)
	}

	r.Step(Message{Type: MsgSnap, From: "n1", To: "n2", Term: 2, Snapshot: &Snapshot{Index: 2, Term: 2}})

	if st := r.Status(); st.SnapshotIndex != 2 || st.LastIndex != 4 {
		t.Fatalf("status = %+v, want snapshot 2 and entries 3..4 kept", st)
	}
}

func TestSnapshotOlderThanCommitIsIgnored(t *testing.T) {
	net := committedLeader(t)
	follower := net.nodes["n2"]
	net.nodes["n1"].Propose([]byte("x"))
	net.deliver()
	net.tickAll()
	net.tickAll()
	before := follower.Status()

	follower.Step(Message{Type: MsgSnap, From: "n1", To: "n2", Term: 1, Snapshot: &Snapshot{Index: 1, Term: 1}})

	if rd := follower.Ready(); rd.Snapshot != nil {
		t.Fatal("follower installed a snapshot older than its commit index")
	}
	if after := follower.Status(); after.LastIndex != before.LastIndex {
		t.Fatalf("log changed: %+v -> %+v", before, after)
	}
}

func TestRestartFromSnapshot(t *testing.T) {
	r, err := New(Config{ID: "n1", Peers: []string{"n1", "n2", "n3"}, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: rand.New(rand.NewPCG(1, 1))}, State{
		HardState: HardState{Term: 3},
		Snapshot:  &Snapshot{Index: 50, Term: 3},
		Entries:   []Entry{{Term: 3, Index: 51}, {Term: 3, Index: 52}},
	})
	if err != nil {
		t.Fatal(err)
	}
	st := r.Status()
	if st.SnapshotIndex != 50 || st.CommitIndex != 50 || st.Applied != 50 || st.LastIndex != 52 {
		t.Fatalf("status = %+v", st)
	}
	if _, err := New(Config{ID: "n1", Peers: []string{"n1"}, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: rand.New(rand.NewPCG(1, 1))}, State{Snapshot: &Snapshot{Index: 50, Term: 3},
		Entries: []Entry{{Term: 3, Index: 60}}}); err == nil {
		t.Fatal("a gap between snapshot and log must be rejected")
	}
}
