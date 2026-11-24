package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

const maxEntriesPerMessage = 64

var (
	ErrNotLeader      = errors.New("not the leader")
	ErrLeaderNotReady = errors.New("leader has not committed an entry in its term yet")
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

type State struct {
	HardState HardState
	Entries   []Entry
}

type Config struct {
	ID             string
	Peers          []string
	ElectionTicks  int
	HeartbeatTicks int
	Rand           *rand.Rand
}

type Ready struct {
	HardState  *HardState
	Entries    []Entry
	Committed  []Entry
	Messages   []Message
	ReadStates []ReadState
}

type ReadState struct {
	ID    uint64
	Index uint64
}

type pendingRead struct {
	id    uint64
	index uint64
	acks  map[string]bool
}

type Status struct {
	ID          string `json:"id"`
	Role        Role   `json:"role"`
	Term        uint64 `json:"term"`
	Leader      string `json:"leader"`
	LastIndex   uint64 `json:"lastIndex"`
	CommitIndex uint64 `json:"commitIndex"`
	Applied     uint64 `json:"applied"`
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

	log         []Entry
	offset      uint64
	offsetTerm  uint64
	commitIndex uint64
	applied     uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	readSeq      uint64
	pendingReads []*pendingRead
	readyReads   []ReadState

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	msgs             []Message
	hardStateChanged bool
	unstableFrom     uint64
}

func New(cfg Config, st State) (*Raft, error) {
	if !slices.Contains(cfg.Peers, cfg.ID) {
		return nil, fmt.Errorf("peers %v must contain %q", cfg.Peers, cfg.ID)
	}
	if cfg.HeartbeatTicks < 1 || cfg.ElectionTicks <= cfg.HeartbeatTicks {
		return nil, errors.New("need 0 < HeartbeatTicks < ElectionTicks")
	}
	if cfg.Rand == nil {
		return nil, errors.New("Rand is required")
	}
	for i, e := range st.Entries {
		if e.Index != uint64(i)+1 {
			return nil, fmt.Errorf("entry %d has index %d, want %d", i, e.Index, i+1)
		}
	}

	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)

	r := &Raft{
		id:             cfg.ID,
		peers:          peers,
		electionTicks:  cfg.ElectionTicks,
		heartbeatTicks: cfg.HeartbeatTicks,
		rand:           cfg.Rand,
		term:           st.HardState.Term,
		votedFor:       st.HardState.VotedFor,
		role:           Follower,
		log:            slices.Clone(st.Entries),
	}
	r.resetElectionTimer()
	return r, nil
}

func (r *Raft) Status() Status {
	return Status{
		ID:          r.id,
		Role:        r.role,
		Term:        r.term,
		Leader:      r.leader,
		LastIndex:   r.lastIndex(),
		CommitIndex: r.commitIndex,
		Applied:     r.applied,
	}
}

func (r *Raft) Ready() Ready {
	var rd Ready
	if r.hardStateChanged {
		rd.HardState = &HardState{Term: r.term, VotedFor: r.votedFor}
		r.hardStateChanged = false
	}
	if r.unstableFrom != 0 {
		rd.Entries = r.entriesFrom(r.unstableFrom, 0)
		r.unstableFrom = 0
	}
	if r.commitIndex > r.applied {
		rd.Committed = r.entriesFrom(r.applied+1, int(r.commitIndex-r.applied))
		r.applied = r.commitIndex
	}
	rd.Messages = r.msgs
	r.msgs = nil
	rd.ReadStates = r.readyReads
	r.readyReads = nil
	return rd
}

func (r *Raft) ReadIndex() (uint64, error) {
	if r.role != Leader {
		return 0, ErrNotLeader
	}
	if t, _ := r.termAt(r.commitIndex); t != r.term {
		return 0, ErrLeaderNotReady
	}

	r.readSeq++
	read := &pendingRead{id: r.readSeq, index: r.commitIndex, acks: map[string]bool{r.id: true}}
	if len(read.acks) >= r.quorum() {
		r.readyReads = append(r.readyReads, ReadState{ID: read.id, Index: read.index})
		return read.id, nil
	}
	r.pendingReads = append(r.pendingReads, read)
	r.broadcastAppend()
	return read.id, nil
}

func (r *Raft) ackReads(from string, context uint64) {
	remaining := r.pendingReads[:0]
	for _, read := range r.pendingReads {
		if read.id <= context {
			read.acks[from] = true
		}
		if len(read.acks) >= r.quorum() {
			r.readyReads = append(r.readyReads, ReadState{ID: read.id, Index: read.index})
			continue
		}
		remaining = append(remaining, read)
	}
	r.pendingReads = remaining
}

func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.role != Leader {
		return 0, 0, ErrNotLeader
	}
	e := Entry{Term: r.term, Index: r.lastIndex() + 1, Data: data}
	r.appendEntry(e)
	r.matchIndex[r.id] = e.Index
	if !r.maybeCommit() {
		r.broadcastAppend()
	}
	return e.Index, e.Term, nil
}

func (r *Raft) Tick() {
	if r.role == Leader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTicks {
			r.heartbeatElapsed = 0
			r.broadcastAppend()
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
			r.send(Message{Type: MsgAppResp, To: m.From, Success: false, RejectHint: r.lastIndex()})
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
		r.handleAppendResponse(m)
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
		r.send(Message{Type: MsgVote, To: p, LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm()})
	}
}

func (r *Raft) handleVote(m Message) {
	canVote := r.votedFor == "" || r.votedFor == m.From
	if !canVote || !r.isUpToDate(m.LastLogIndex, m.LastLogTerm) {
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

	if m.PrevLogIndex < r.commitIndex {
		r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: r.commitIndex, ReadContext: m.ReadContext})
		return
	}
	if prevTerm, ok := r.termAt(m.PrevLogIndex); !ok || prevTerm != m.PrevLogTerm {
		hint := min(r.lastIndex(), m.PrevLogIndex-1)
		r.send(Message{Type: MsgAppResp, To: m.From, Success: false, RejectHint: hint, ReadContext: m.ReadContext})
		return
	}

	for _, e := range m.Entries {
		if e.Index <= r.lastIndex() {
			if t, _ := r.termAt(e.Index); t == e.Term {
				continue
			}
			r.truncateFrom(e.Index)
		}
		r.appendEntry(e)
	}

	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if commit := min(m.LeaderCommit, lastNew); commit > r.commitIndex {
		r.commitIndex = commit
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew, ReadContext: m.ReadContext})
}

func (r *Raft) handleAppendResponse(m Message) {
	if r.role != Leader {
		return
	}
	r.ackReads(m.From, m.ReadContext)
	if !m.Success {
		if r.nextIndex[m.From] <= r.matchIndex[m.From]+1 {
			return
		}
		r.nextIndex[m.From] = max(1, min(r.nextIndex[m.From]-1, m.RejectHint+1))
		r.sendAppend(m.From)
		return
	}

	if m.MatchIndex > r.matchIndex[m.From] {
		r.matchIndex[m.From] = m.MatchIndex
	}
	if m.MatchIndex+1 > r.nextIndex[m.From] {
		r.nextIndex[m.From] = m.MatchIndex + 1
	}
	r.maybeCommit()
	if r.nextIndex[m.From] <= r.lastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) maybeCommit() bool {
	matched := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		matched = append(matched, r.matchIndex[p])
	}
	slices.Sort(matched)
	slices.Reverse(matched)
	n := matched[r.quorum()-1]

	if n <= r.commitIndex {
		return false
	}
	if t, _ := r.termAt(n); t != r.term {
		return false
	}
	r.commitIndex = n
	return true
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
	r.nextIndex = nil
	r.matchIndex = nil
	r.pendingReads = nil
	r.resetElectionTimer()
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leader = r.id
	r.votes = nil
	r.heartbeatElapsed = 0

	r.nextIndex = map[string]uint64{}
	r.matchIndex = map[string]uint64{}
	for _, p := range r.peers {
		r.nextIndex[p] = r.lastIndex() + 1
	}

	noop := Entry{Term: r.term, Index: r.lastIndex() + 1}
	r.appendEntry(noop)
	r.matchIndex[r.id] = noop.Index
	if !r.maybeCommit() {
		r.broadcastAppend()
	}
}

func (r *Raft) broadcastAppend() {
	for _, p := range r.others() {
		r.sendAppend(p)
	}
}

func (r *Raft) sendAppend(to string) {
	prev := r.nextIndex[to] - 1
	prevTerm, _ := r.termAt(prev)
	r.send(Message{
		Type:         MsgApp,
		To:           to,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      r.entriesFrom(prev+1, maxEntriesPerMessage),
		LeaderCommit: r.commitIndex,
		ReadContext:  r.readSeq,
	})
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
