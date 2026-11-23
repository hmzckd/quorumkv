package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"votedFor"`
}

type Config struct {
	ID             string
	Peers          []string
	ElectionTicks  int
	HeartbeatTicks int
	Rand           *rand.Rand
}

type Ready struct {
	HardState *HardState
	Messages  []Message
}

type Status struct {
	ID     string `json:"id"`
	Role   Role   `json:"role"`
	Term   uint64 `json:"term"`
	Leader string `json:"leader"`
}

type Raft struct {
	id             string
	peers          []string
	electionTicks  int
	heartbeatTicks int
	rand           *rand.Rand

	term     uint64
	votedFor string
	role     Role
	leader   string
	votes    map[string]bool

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	msgs             []Message
	hardStateChanged bool
}

func New(cfg Config, hs HardState) (*Raft, error) {
	if !slices.Contains(cfg.Peers, cfg.ID) {
		return nil, fmt.Errorf("peers %v must contain %q", cfg.Peers, cfg.ID)
	}
	if cfg.HeartbeatTicks < 1 || cfg.ElectionTicks <= cfg.HeartbeatTicks {
		return nil, errors.New("need 0 < HeartbeatTicks < ElectionTicks")
	}
	if cfg.Rand == nil {
		return nil, errors.New("Rand is required")
	}

	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)

	r := &Raft{
		id:             cfg.ID,
		peers:          peers,
		electionTicks:  cfg.ElectionTicks,
		heartbeatTicks: cfg.HeartbeatTicks,
		rand:           cfg.Rand,
		term:           hs.Term,
		votedFor:       hs.VotedFor,
		role:           Follower,
	}
	r.resetElectionTimer()
	return r, nil
}

func (r *Raft) Status() Status {
	return Status{ID: r.id, Role: r.role, Term: r.term, Leader: r.leader}
}

func (r *Raft) Ready() Ready {
	var rd Ready
	if r.hardStateChanged {
		rd.HardState = &HardState{Term: r.term, VotedFor: r.votedFor}
		r.hardStateChanged = false
	}
	rd.Messages = r.msgs
	r.msgs = nil
	return rd
}

func (r *Raft) Tick() {
	if r.role == Leader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTicks {
			r.heartbeatElapsed = 0
			r.broadcastHeartbeat()
		}
		return
	}

	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.campaign()
	}
}

func (r *Raft) Step(m Message) {
	if m.To != r.id {
		return
	}

	switch {
	case m.Term > r.term:
		leader := ""
		if m.Type == MsgApp {
			leader = m.From
		}
		r.becomeFollower(m.Term, leader)
	case m.Term < r.term:
		switch m.Type {
		case MsgVote:
			r.send(Message{Type: MsgVoteResp, To: m.From, Granted: false})
		case MsgApp:
			r.send(Message{Type: MsgAppResp, To: m.From, Success: false})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		r.handleVote(m)
	case MsgVoteResp:
		r.handleVoteResponse(m)
	case MsgApp:
		r.handleAppend(m)
	case MsgAppResp:
	}
}

func (r *Raft) campaign() {
	r.term++
	r.votedFor = r.id
	r.hardStateChanged = true
	r.role = Candidate
	r.leader = ""
	r.votes = map[string]bool{r.id: true}
	r.resetElectionTimer()

	if r.grantedVotes() >= r.quorum() {
		r.becomeLeader()
		return
	}
	for _, p := range r.others() {
		r.send(Message{Type: MsgVote, To: p})
	}
}

func (r *Raft) handleVote(m Message) {
	canVote := r.votedFor == "" || r.votedFor == m.From
	if !canVote {
		r.send(Message{Type: MsgVoteResp, To: m.From, Granted: false})
		return
	}
	if r.votedFor != m.From {
		r.votedFor = m.From
		r.hardStateChanged = true
	}
	r.electionElapsed = 0
	r.send(Message{Type: MsgVoteResp, To: m.From, Granted: true})
}

func (r *Raft) handleVoteResponse(m Message) {
	if r.role != Candidate {
		return
	}
	r.votes[m.From] = m.Granted
	if r.grantedVotes() >= r.quorum() {
		r.becomeLeader()
	}
}

func (r *Raft) handleAppend(m Message) {
	if r.role != Follower {
		r.becomeFollower(r.term, m.From)
	}
	r.leader = m.From
	r.electionElapsed = 0
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true})
}

func (r *Raft) becomeFollower(term uint64, leader string) {
	if term != r.term {
		r.term = term
		r.votedFor = ""
		r.hardStateChanged = true
	}
	r.role = Follower
	r.leader = leader
	r.votes = nil
	r.resetElectionTimer()
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leader = r.id
	r.votes = nil
	r.heartbeatElapsed = 0
	r.broadcastHeartbeat()
}

func (r *Raft) broadcastHeartbeat() {
	for _, p := range r.others() {
		r.send(Message{Type: MsgApp, To: p})
	}
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.electionTimeout = r.electionTicks + r.rand.IntN(r.electionTicks)
}

func (r *Raft) send(m Message) {
	m.From = r.id
	m.Term = r.term
	r.msgs = append(r.msgs, m)
}

func (r *Raft) grantedVotes() int {
	n := 0
	for _, granted := range r.votes {
		if granted {
			n++
		}
	}
	return n
}

func (r *Raft) quorum() int {
	return len(r.peers)/2 + 1
}

func (r *Raft) others() []string {
	out := make([]string, 0, len(r.peers)-1)
	for _, p := range r.peers {
		if p != r.id {
			out = append(out, p)
		}
	}
	return out
}
