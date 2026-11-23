package raft

type MsgType string

const (
	MsgVote     MsgType = "RequestVote"
	MsgVoteResp MsgType = "RequestVoteResponse"
	MsgApp      MsgType = "AppendEntries"
	MsgAppResp  MsgType = "AppendEntriesResponse"
)

type Entry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Data  []byte `json:"data,omitempty"`
}

type Message struct {
	Type MsgType `json:"type"`
	From string  `json:"from"`
	To   string  `json:"to"`
	Term uint64  `json:"term"`

	LastLogIndex uint64 `json:"lastLogIndex,omitempty"`
	LastLogTerm  uint64 `json:"lastLogTerm,omitempty"`
	Granted      bool   `json:"granted,omitempty"`

	PrevLogIndex uint64  `json:"prevLogIndex,omitempty"`
	PrevLogTerm  uint64  `json:"prevLogTerm,omitempty"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leaderCommit,omitempty"`

	Success    bool   `json:"success,omitempty"`
	MatchIndex uint64 `json:"matchIndex,omitempty"`
	RejectHint uint64 `json:"rejectHint,omitempty"`
}
