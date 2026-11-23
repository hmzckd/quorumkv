package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/hmzckd/quorumkv/internal/kv"
	"github.com/hmzckd/quorumkv/internal/raft"
	"github.com/hmzckd/quorumkv/internal/storage"
)

var (
	ErrStopped        = errors.New("node stopped")
	ErrUnknownOutcome = errors.New("outcome unknown: the write may or may not have been applied, retry it")
	ErrNotCommitted   = errors.New("the write was overwritten by a new leader and not applied, retry it")
)

type NotLeaderError struct {
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "no leader is known right now"
	}
	return "not the leader, the leader is " + e.Leader
}

type Status struct {
	raft.Status
	Keys int `json:"keys"`
}

type proposal struct {
	data  []byte
	term  uint64
	reply chan error
}

type Sender interface {
	Send([]raft.Message)
}

type Config struct {
	ID             string
	Peers          []string
	DataDir        string
	Tick           time.Duration
	ElectionTicks  int
	HeartbeatTicks int
}

type Node struct {
	id        string
	tick      time.Duration
	core      *raft.Raft
	store     *storage.Storage
	sender    Sender
	logger    *slog.Logger
	kv        *kv.Store
	inbox     chan raft.Message
	proposals chan *proposal
	statusReq chan chan Status
	done      chan struct{}
	last      raft.Status
	pending   map[uint64]*proposal
}

func New(cfg Config, sender Sender, logger *slog.Logger) (*Node, error) {
	store, loaded, err := storage.Open(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}
	if loaded.CutBytes > 0 {
		logger.Warn("cut damaged log tail", "bytes", loaded.CutBytes, "reason", loaded.CutReason)
	}

	seed := uint64(time.Now().UnixNano())
	core, err := raft.New(raft.Config{
		ID:             cfg.ID,
		Peers:          cfg.Peers,
		ElectionTicks:  cfg.ElectionTicks,
		HeartbeatTicks: cfg.HeartbeatTicks,
		Rand:           rand.New(rand.NewPCG(seed, seed>>1)),
	}, raft.State{HardState: loaded.HardState, Entries: loaded.Entries})
	if err != nil {
		store.Close()
		return nil, err
	}
	logger.Info("loaded from disk", "term", loaded.HardState.Term, "voted_for", loaded.HardState.VotedFor,
		"entries", len(loaded.Entries))

	return &Node{
		id:        cfg.ID,
		tick:      cfg.Tick,
		core:      core,
		store:     store,
		sender:    sender,
		logger:    logger,
		kv:        kv.New(),
		inbox:     make(chan raft.Message, 4096),
		proposals: make(chan *proposal),
		statusReq: make(chan chan Status),
		done:      make(chan struct{}),
		last:      core.Status(),
		pending:   map[uint64]*proposal{},
	}, nil
}

func (n *Node) Deliver(m raft.Message) bool {
	select {
	case n.inbox <- m:
		return true
	default:
		return false
	}
}

func (n *Node) Status(ctx context.Context) (Status, error) {
	reply := make(chan Status, 1)
	select {
	case n.statusReq <- reply:
	case <-n.done:
		return Status{}, ErrStopped
	case <-ctx.Done():
		return Status{}, ctx.Err()
	}
	select {
	case st := <-reply:
		return st, nil
	case <-ctx.Done():
		return Status{}, ctx.Err()
	}
}

func (n *Node) Propose(ctx context.Context, cmd kv.Command) error {
	p := &proposal{data: cmd.Encode(), reply: make(chan error, 1)}
	select {
	case n.proposals <- p:
	case <-n.done:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-p.reply:
		return err
	case <-n.done:
		return ErrUnknownOutcome
	case <-ctx.Done():
		return ErrUnknownOutcome
	}
}

func (n *Node) Run(ctx context.Context) error {
	defer close(n.done)
	defer n.store.Close()

	ticker := time.NewTicker(n.tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n.core.Tick()
		case m := <-n.inbox:
			n.core.Step(m)
		case p := <-n.proposals:
			n.propose(p)
		case reply := <-n.statusReq:
			reply <- Status{Status: n.core.Status(), Keys: n.kv.Len()}
		}
		if err := n.handleReady(); err != nil {
			return err
		}
	}
}

func (n *Node) handleReady() error {
	rd := n.core.Ready()
	if rd.HardState != nil {
		if err := n.store.SaveHardState(*rd.HardState); err != nil {
			return fmt.Errorf("save hard state: %w", err)
		}
	}
	if len(rd.Entries) > 0 {
		if err := n.store.Append(rd.Entries); err != nil {
			return fmt.Errorf("append to log: %w", err)
		}
	}
	n.sender.Send(rd.Messages)
	for _, e := range rd.Committed {
		n.apply(e)
	}
	n.logChanges()
	return nil
}

func (n *Node) propose(p *proposal) {
	index, term, err := n.core.Propose(p.data)
	if err != nil {
		p.reply <- &NotLeaderError{Leader: n.core.Status().Leader}
		return
	}
	p.term = term
	n.pending[index] = p
}

func (n *Node) apply(e raft.Entry) {
	if err := n.kv.Apply(e.Data); err != nil {
		n.logger.Error("skipped entry that cannot be applied", "index", e.Index, "error", err)
	}
	p, ok := n.pending[e.Index]
	if !ok {
		return
	}
	delete(n.pending, e.Index)
	if p.term == e.Term {
		p.reply <- nil
	} else {
		p.reply <- ErrNotCommitted
	}
}

func (n *Node) failPending(err error) {
	for index, p := range n.pending {
		p.reply <- err
		delete(n.pending, index)
	}
}

func (n *Node) logChanges() {
	st := n.core.Status()
	if st.Role != n.last.Role || st.Term != n.last.Term || st.Leader != n.last.Leader {
		n.logger.Info("raft state changed", "role", st.Role, "term", st.Term, "leader", st.Leader)
	}
	if n.last.Role == raft.Leader && st.Role != raft.Leader {
		n.failPending(ErrUnknownOutcome)
	}
	n.last = st
}
