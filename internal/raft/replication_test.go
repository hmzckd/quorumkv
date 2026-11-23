package raft

import (
	"slices"
	"testing"
)

func electN1(t *testing.T, net *network) *Raft {
	t.Helper()
	tickUntilCandidate(t, net.nodes["n1"])
	net.deliver()
	if net.nodes["n1"].Status().Role != Leader {
		t.Fatal("n1 did not become leader")
	}
	return net.nodes["n1"]
}

func logOf(r *Raft) []Entry {
	return r.entriesFrom(1, 0)
}

func TestLeaderAppendsNoopWhenElected(t *testing.T) {
	net := newNetwork(t, "n1", "n2", "n3")
	leader := electN1(t, net)

	log := logOf(leader)
	if len(log) != 1 || log[0].Term != 1 || log[0].Index != 1 || log[0].Data != nil {
		t.Fatalf("leader log = %+v, want one empty entry of term 1", log)
	}
	if st := leader.Status(); st.CommitIndex != 1 {
		t.Errorf("commit = %d, want 1: the no-op is on all three nodes", st.CommitIndex)
	}
}

func TestProposeOnFollowerIsRejected(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)
	if _, _, err := r.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

func TestEntryCommitsOnMajorityAndReachesFollowers(t *testing.T) {
	net := newNetwork(t, "n1", "n2", "n3")
	leader := electN1(t, net)
	net.down["n3"] = true

	index, term, err := leader.Propose([]byte("x=1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	net.deliver()

	if index != 2 || term != 1 {
		t.Fatalf("proposed at index %d term %d, want 2 and 1", index, term)
	}
	if st := leader.Status(); st.CommitIndex != 2 {
		t.Fatalf("leader commit = %d, want 2: n1 and n2 are a majority", st.CommitIndex)
	}

	for i := 0; i < heartbeatTicks; i++ {
		net.tickAll()
	}
	if st := net.nodes["n2"].Status(); st.CommitIndex != 2 {
		t.Errorf("n2 commit = %d, want 2 after the next heartbeat", st.CommitIndex)
	}
	if st := net.nodes["n3"].Status(); st.LastIndex != 1 {
		t.Errorf("n3 last index = %d, want 1: it was down", st.LastIndex)
	}
}

func TestCommittedEntriesComeOutOfReadyOnce(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1"}, 1)
	for r.Status().Role != Leader {
		r.Tick()
	}
	r.Ready()

	r.Propose([]byte("a"))
	r.Propose([]byte("b"))
	rd := r.Ready()

	if len(rd.Entries) != 2 {
		t.Fatalf("entries to persist = %d, want 2", len(rd.Entries))
	}
	if got := len(rd.Committed); got != 2 || string(rd.Committed[1].Data) != "b" {
		t.Fatalf("committed = %+v, want a and b", rd.Committed)
	}
	if rd2 := r.Ready(); len(rd2.Entries) != 0 || len(rd2.Committed) != 0 {
		t.Fatalf("second Ready repeated work: %+v", rd2)
	}
}

func TestFollowerRejectsAppendWhenPrevIsMissing(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)

	r.Step(Message{Type: MsgApp, From: "n2", To: "n1", Term: 1, PrevLogIndex: 5, PrevLogTerm: 1,
		Entries: []Entry{{Term: 1, Index: 6}}})

	m := r.Ready().Messages[0]
	if m.Success || m.RejectHint != 0 {
		t.Fatalf("response = %+v, want reject with hint 0 (my log is empty)", m)
	}
	if st := r.Status(); st.LastIndex != 0 {
		t.Errorf("last index = %d, want 0", st.LastIndex)
	}
}

func TestFollowerReplacesConflictingEntries(t *testing.T) {
	r, err := New(Config{ID: "n1", Peers: []string{"n1", "n2", "n3"}, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: newTestRaft(t, "n1", []string{"n1"}, 1).rand}, State{
		HardState: HardState{Term: 2},
		Entries:   []Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 2, Index: 3, Data: []byte("lost")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	r.Step(Message{Type: MsgApp, From: "n2", To: "n1", Term: 3, PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []Entry{{Term: 3, Index: 3, Data: []byte("kept")}}})

	rd := r.Ready()
	if len(rd.Entries) != 1 || rd.Entries[0].Index != 3 || string(rd.Entries[0].Data) != "kept" {
		t.Fatalf("entries to persist = %+v, want index 3 replaced by kept", rd.Entries)
	}
	if got := logOf(r); len(got) != 3 || string(got[2].Data) != "kept" {
		t.Fatalf("log = %+v", got)
	}
	if !rd.Messages[0].Success || rd.Messages[0].MatchIndex != 3 {
		t.Errorf("response = %+v, want success with match 3", rd.Messages[0])
	}
}

func TestDuplicateAppendDoesNotTruncate(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)
	full := Message{Type: MsgApp, From: "n2", To: "n1", Term: 1,
		Entries: []Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}}
	old := Message{Type: MsgApp, From: "n2", To: "n1", Term: 1, Entries: []Entry{{Term: 1, Index: 1}}}

	r.Step(full)
	r.Ready()
	r.Step(old)

	if rd := r.Ready(); len(rd.Entries) != 0 {
		t.Fatalf("a late duplicate rewrote entries %+v", rd.Entries)
	}
	if st := r.Status(); st.LastIndex != 3 {
		t.Errorf("last index = %d, want 3", st.LastIndex)
	}
}

func TestVoteDeniedToCandidateWithOlderLog(t *testing.T) {
	r, err := New(Config{ID: "n1", Peers: []string{"n1", "n2", "n3"}, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: newTestRaft(t, "n1", []string{"n1"}, 1).rand}, State{
		HardState: HardState{Term: 2},
		Entries:   []Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}

	r.Step(Message{Type: MsgVote, From: "n2", To: "n1", Term: 3, LastLogIndex: 5, LastLogTerm: 1})
	r.Step(Message{Type: MsgVote, From: "n3", To: "n1", Term: 3, LastLogIndex: 1, LastLogTerm: 2})
	r.Step(Message{Type: MsgVote, From: "n3", To: "n1", Term: 4, LastLogIndex: 2, LastLogTerm: 2})

	msgs := r.Ready().Messages
	want := []bool{false, false, true}
	for i, m := range msgs {
		if m.Granted != want[i] {
			t.Errorf("vote %d to %s granted=%v, want %v", i, m.To, m.Granted, want[i])
		}
	}
}

func TestLeaderOnlyCommitsEntriesOfItsOwnTerm(t *testing.T) {
	r, err := New(Config{ID: "n1", Peers: []string{"n1", "n2", "n3"}, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: newTestRaft(t, "n1", []string{"n1"}, 1).rand}, State{
		HardState: HardState{Term: 2},
		Entries:   []Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tickUntilCandidate(t, r)
	r.Step(Message{Type: MsgVoteResp, From: "n2", To: "n1", Term: 3, Granted: true})
	if r.Status().Role != Leader {
		t.Fatal("n1 is not leader")
	}
	r.Ready()

	r.Step(Message{Type: MsgAppResp, From: "n2", To: "n1", Term: 3, Success: true, MatchIndex: 2})
	if c := r.Status().CommitIndex; c != 0 {
		t.Fatalf("commit = %d, want 0: index 2 is from term 2 and may still be overwritten", c)
	}

	r.Step(Message{Type: MsgAppResp, From: "n2", To: "n1", Term: 3, Success: true, MatchIndex: 3})
	if c := r.Status().CommitIndex; c != 3 {
		t.Fatalf("commit = %d, want 3: the term 3 no-op on a majority commits everything before it", c)
	}
}

func TestLeaderBringsLaggingFollowerUpToDate(t *testing.T) {
	net := newNetwork(t, "n1", "n2", "n3")
	leader := electN1(t, net)
	net.down["n3"] = true
	for i := 0; i < 100; i++ {
		leader.Propose([]byte{byte(i)})
		net.deliver()
	}

	net.down["n3"] = false
	for i := 0; i < 20; i++ {
		net.tickAll()
	}

	if got, want := logOf(net.nodes["n3"]), logOf(leader); !slices.EqualFunc(got, want, sameEntry) {
		t.Fatalf("n3 has %d entries, leader has %d", len(got), len(want))
	}
	if st := net.nodes["n3"].Status(); st.CommitIndex != 101 {
		t.Errorf("n3 commit = %d, want 101", st.CommitIndex)
	}
}

func sameEntry(a, b Entry) bool {
	return a.Term == b.Term && a.Index == b.Index && string(a.Data) == string(b.Data)
}
