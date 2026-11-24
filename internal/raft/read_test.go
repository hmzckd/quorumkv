package raft

import "testing"

func committedLeader(t *testing.T) *network {
	t.Helper()
	net := newNetwork(t, "n1", "n2", "n3")
	electN1(t, net)
	if c := net.nodes["n1"].Status().CommitIndex; c != 1 {
		t.Fatalf("commit = %d, want the no-op committed", c)
	}
	return net
}

func TestReadIndexIsConfirmedByAMajority(t *testing.T) {
	net := committedLeader(t)
	leader := net.nodes["n1"]
	leader.Propose([]byte("x=1"))
	net.deliver()

	id, err := leader.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	rd := leader.Ready()
	if len(rd.ReadStates) != 0 {
		t.Fatalf("read confirmed before anyone answered: %+v", rd.ReadStates)
	}
	for _, m := range rd.Messages {
		if m.ReadContext != id {
			t.Errorf("heartbeat to %s carries context %d, want %d", m.To, m.ReadContext, id)
		}
		net.nodes[m.To].Step(m)
	}

	reply := net.nodes["n2"].Ready().Messages[0]
	if reply.ReadContext != id {
		t.Fatalf("follower echoed context %d, want %d", reply.ReadContext, id)
	}
	leader.Step(reply)

	got := leader.Ready().ReadStates
	if len(got) != 1 || got[0] != (ReadState{ID: id, Index: 2}) {
		t.Fatalf("read states = %+v, want {%d 2}", got, id)
	}
}

func TestOldHeartbeatReplyDoesNotConfirmANewRead(t *testing.T) {
	net := committedLeader(t)
	leader := net.nodes["n1"]
	leader.Tick()
	leader.Tick()
	beat := leader.Ready().Messages
	net.nodes["n2"].Step(beat[0])
	oldReply := net.nodes["n2"].Ready().Messages[0]

	if _, err := leader.ReadIndex(); err != nil {
		t.Fatal(err)
	}
	leader.Ready()
	leader.Step(oldReply)

	if got := leader.Ready().ReadStates; len(got) != 0 {
		t.Fatalf("a reply to a heartbeat sent before the read confirmed it: %+v", got)
	}
}

func TestReadIndexOnFollowerFails(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)
	if _, err := r.ReadIndex(); err != ErrNotLeader {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
}

func TestNewLeaderMustCommitBeforeServingReads(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)
	tickUntilCandidate(t, r)
	r.Step(Message{Type: MsgVoteResp, From: "n2", To: "n1", Term: 1, Granted: true})
	if r.Status().Role != Leader {
		t.Fatal("not leader")
	}

	if _, err := r.ReadIndex(); err != ErrLeaderNotReady {
		t.Fatalf("err = %v, want ErrLeaderNotReady: the new leader does not know the commit index yet", err)
	}
}

func TestSingleNodeReadIsImmediate(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1"}, 1)
	for r.Status().Role != Leader {
		r.Tick()
	}
	r.Ready()

	id, err := r.ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Ready().ReadStates; len(got) != 1 || got[0].ID != id {
		t.Fatalf("read states = %+v", got)
	}
}

func TestSteppingDownDropsPendingReads(t *testing.T) {
	net := committedLeader(t)
	leader := net.nodes["n1"]
	leader.ReadIndex()
	leader.Ready()

	leader.Step(Message{Type: MsgVote, From: "n3", To: "n1", Term: 5, LastLogIndex: 9, LastLogTerm: 4})
	leader.Step(Message{Type: MsgAppResp, From: "n2", To: "n1", Term: 5, Success: true, ReadContext: 99})

	if got := leader.Ready().ReadStates; len(got) != 0 {
		t.Fatalf("a deposed leader confirmed a read: %+v", got)
	}
}
