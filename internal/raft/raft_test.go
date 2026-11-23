package raft

import (
	"math/rand/v2"
	"testing"
)

const (
	electionTicks  = 10
	heartbeatTicks = 2
)

func newTestRaft(t *testing.T, id string, peers []string, seed uint64) *Raft {
	t.Helper()
	r, err := New(Config{
		ID:             id,
		Peers:          peers,
		ElectionTicks:  electionTicks,
		HeartbeatTicks: heartbeatTicks,
		Rand:           rand.New(rand.NewPCG(seed, seed)),
	}, State{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

type network struct {
	t     *testing.T
	nodes map[string]*Raft
	order []string
	down  map[string]bool
	log   bool
}

func newNetwork(t *testing.T, ids ...string) *network {
	n := &network{t: t, nodes: map[string]*Raft{}, order: ids, down: map[string]bool{}}
	for i, id := range ids {
		n.nodes[id] = newTestRaft(t, id, ids, uint64(i+1))
	}
	return n
}

func (n *network) deliver() {
	for {
		var pending []Message
		for _, id := range n.order {
			pending = append(pending, n.nodes[id].Ready().Messages...)
		}
		if len(pending) == 0 {
			return
		}
		for _, m := range pending {
			if n.down[m.From] || n.down[m.To] {
				continue
			}
			if n.log && m.Type == MsgVote {
				n.t.Logf("  %s -> %s %s term=%d", m.From, m.To, m.Type, m.Term)
			}
			if n.log && m.Type == MsgVoteResp {
				n.t.Logf("  %s -> %s %s term=%d granted=%v", m.From, m.To, m.Type, m.Term, m.Granted)
			}
			n.nodes[m.To].Step(m)
		}
	}
}

func (n *network) tickAll() {
	for _, id := range n.order {
		if !n.down[id] {
			n.nodes[id].Tick()
		}
	}
	n.deliver()
}

func (n *network) leaders() []string {
	var out []string
	for _, id := range n.order {
		if n.nodes[id].Status().Role == Leader {
			out = append(out, id)
		}
	}
	return out
}

func tickUntilCandidate(t *testing.T, r *Raft) {
	t.Helper()
	for i := 0; i < 2*electionTicks; i++ {
		r.Tick()
		if r.Status().Role == Candidate {
			return
		}
	}
	t.Fatalf("%s did not start an election within %d ticks", r.id, 2*electionTicks)
}

func TestNewRejectsBadConfig(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 1))
	cases := map[string]Config{
		"self missing":        {ID: "n9", Peers: []string{"n1"}, ElectionTicks: 10, HeartbeatTicks: 2, Rand: rnd},
		"heartbeat zero":      {ID: "n1", Peers: []string{"n1"}, ElectionTicks: 10, HeartbeatTicks: 0, Rand: rnd},
		"election too short":  {ID: "n1", Peers: []string{"n1"}, ElectionTicks: 2, HeartbeatTicks: 2, Rand: rnd},
		"no random generator": {ID: "n1", Peers: []string{"n1"}, ElectionTicks: 10, HeartbeatTicks: 2},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := New(cfg, State{}); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestFollowerStartsElectionAfterTimeout(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)

	tickUntilCandidate(t, r)

	st := r.Status()
	if st.Term != 1 {
		t.Errorf("term = %d, want 1", st.Term)
	}
	rd := r.Ready()
	if rd.HardState == nil || *rd.HardState != (HardState{Term: 1, VotedFor: "n1"}) {
		t.Errorf("hard state = %+v, want term 1 voted for n1", rd.HardState)
	}
	if len(rd.Messages) != 2 {
		t.Fatalf("sent %d messages, want 2 vote requests", len(rd.Messages))
	}
	for _, m := range rd.Messages {
		if m.Type != MsgVote || m.Term != 1 {
			t.Errorf("message = %+v, want RequestVote term 1", m)
		}
	}
}

func TestElectionTimeoutIsRandomized(t *testing.T) {
	seen := map[int]bool{}
	for seed := uint64(1); seed <= 50; seed++ {
		r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, seed)
		if r.electionTimeout < electionTicks || r.electionTimeout >= 2*electionTicks {
			t.Fatalf("timeout %d outside [%d, %d)", r.electionTimeout, electionTicks, 2*electionTicks)
		}
		seen[r.electionTimeout] = true
	}
	if len(seen) < 5 {
		t.Errorf("only %d distinct timeouts across 50 seeds, want variety", len(seen))
	}
}

func TestCandidateWithMajorityBecomesLeader(t *testing.T) {
	net := newNetwork(t, "n1", "n2", "n3")

	tickUntilCandidate(t, net.nodes["n1"])
	net.deliver()

	if got := net.leaders(); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("leaders = %v, want [n1]", got)
	}
	for _, id := range []string{"n2", "n3"} {
		st := net.nodes[id].Status()
		if st.Role != Follower || st.Leader != "n1" || st.Term != 1 {
			t.Errorf("%s status = %+v, want follower of n1 in term 1", id, st)
		}
	}
}

func TestNodeVotesOncePerTerm(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)

	r.Step(Message{Type: MsgVote, From: "n2", To: "n1", Term: 1})
	r.Step(Message{Type: MsgVote, From: "n3", To: "n1", Term: 1})
	r.Step(Message{Type: MsgVote, From: "n2", To: "n1", Term: 1})

	msgs := r.Ready().Messages
	want := []bool{true, false, true}
	if len(msgs) != len(want) {
		t.Fatalf("got %d responses, want %d", len(msgs), len(want))
	}
	for i, m := range msgs {
		if m.Type != MsgVoteResp || m.Granted != want[i] {
			t.Errorf("response %d to %s: granted=%v, want %v", i, m.To, m.Granted, want[i])
		}
	}
}

func TestVoteIsKeptAcrossRestart(t *testing.T) {
	peers := []string{"n1", "n2", "n3"}
	r := newTestRaft(t, "n1", peers, 1)
	r.Step(Message{Type: MsgVote, From: "n2", To: "n1", Term: 4})
	saved := r.Ready().HardState

	restarted, err := New(Config{ID: "n1", Peers: peers, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: rand.New(rand.NewPCG(9, 9))}, State{HardState: *saved})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	restarted.Step(Message{Type: MsgVote, From: "n3", To: "n1", Term: 4})

	if m := restarted.Ready().Messages[0]; m.Granted {
		t.Fatal("restarted node voted twice in term 4")
	}
}

func TestStaleTermIsRejected(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)
	r.Step(Message{Type: MsgApp, From: "n2", To: "n1", Term: 5})
	r.Ready()

	r.Step(Message{Type: MsgVote, From: "n3", To: "n1", Term: 3})
	r.Step(Message{Type: MsgApp, From: "n3", To: "n1", Term: 3})

	msgs := r.Ready().Messages
	if len(msgs) != 2 {
		t.Fatalf("got %d responses, want 2", len(msgs))
	}
	if msgs[0].Granted || msgs[1].Success {
		t.Errorf("stale requests were accepted: %+v", msgs)
	}
	for _, m := range msgs {
		if m.Term != 5 {
			t.Errorf("response term = %d, want 5 so the sender learns it is stale", m.Term)
		}
	}
	if st := r.Status(); st.Leader != "n2" || st.Term != 5 {
		t.Errorf("status = %+v, want follower of n2 in term 5", st)
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	net := newNetwork(t, "n1", "n2", "n3")
	tickUntilCandidate(t, net.nodes["n1"])
	net.deliver()

	net.nodes["n1"].Step(Message{Type: MsgVote, From: "n3", To: "n1", Term: 7})

	st := net.nodes["n1"].Status()
	if st.Role != Follower || st.Term != 7 {
		t.Fatalf("status = %+v, want follower in term 7", st)
	}
}

func TestCandidateStepsDownForLeaderOfSameTerm(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1", "n2", "n3"}, 1)
	tickUntilCandidate(t, r)

	r.Step(Message{Type: MsgApp, From: "n2", To: "n1", Term: 1})

	st := r.Status()
	if st.Role != Follower || st.Leader != "n2" || st.Term != 1 {
		t.Fatalf("status = %+v, want follower of n2 in term 1", st)
	}
}

func TestHeartbeatsKeepTheLeader(t *testing.T) {
	net := newNetwork(t, "n1", "n2", "n3")
	tickUntilCandidate(t, net.nodes["n1"])
	net.deliver()

	for i := 0; i < 1000; i++ {
		net.tickAll()
	}

	if got := net.leaders(); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("leaders = %v, want [n1]", got)
	}
	if term := net.nodes["n2"].Status().Term; term != 1 {
		t.Errorf("term = %d, want 1: no new election should start while heartbeats arrive", term)
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	r := newTestRaft(t, "n1", []string{"n1"}, 1)
	for i := 0; i < 2*electionTicks; i++ {
		r.Tick()
	}
	if st := r.Status(); st.Role != Leader || st.Term != 1 {
		t.Fatalf("status = %+v, want leader in term 1", st)
	}
}

func TestElectionStory(t *testing.T) {
	net := newNetwork(t, "n1", "n2", "n3")
	net.log = true

	for tick := 1; tick <= 40; tick++ {
		before := net.leaders()
		net.tickAll()
		if after := net.leaders(); len(after) == 1 && len(before) == 0 {
			st := net.nodes[after[0]].Status()
			t.Logf("tick %d: %s is leader in term %d", tick, st.ID, st.Term)
		}
	}
	if len(net.leaders()) != 1 {
		t.Fatalf("leaders = %v, want exactly one", net.leaders())
	}
}
