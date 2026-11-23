package raft

type MsgType string

const (
	MsgVote     MsgType = "RequestVote"
	MsgVoteResp MsgType = "RequestVoteResponse"
	MsgApp      MsgType = "AppendEntries"
	MsgAppResp  MsgType = "AppendEntriesResponse"
)

type Message struct {
	Type    MsgType `json:"type"`
	From    string  `json:"from"`
	To      string  `json:"to"`
	Term    uint64  `json:"term"`
	Granted bool    `json:"granted,omitempty"`
	Success bool    `json:"success,omitempty"`
}
