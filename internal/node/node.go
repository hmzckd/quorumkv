package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/hmzckd/quorumkv/internal/raft"
	"github.com/hmzckd/quorumkv/internal/storage"
)

var ErrStopped = errors.New("node stopped")

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
	inbox     chan raft.Message
	statusReq chan chan raft.Status
	done      chan struct{}
	last      raft.Status
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
		inbox:     make(chan raft.Message, 4096),
		statusReq: make(chan chan raft.Status),
		done:      make(chan struct{}),
		last:      core.Status(),
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

func (n *Node) Status(ctx context.Context) (raft.Status, error) {
	reply := make(chan raft.Status, 1)
	select {
	case n.statusReq <- reply:
	case <-n.done:
		return raft.Status{}, ErrStopped
	case <-ctx.Done():
		return raft.Status{}, ctx.Err()
	}
	select {
	case st := <-reply:
		return st, nil
	case <-ctx.Done():
		return raft.Status{}, ctx.Err()
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
		case reply := <-n.statusReq:
			reply <- n.core.Status()
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
	n.logChanges()
	return nil
}

func (n *Node) logChanges() {
	st := n.core.Status()
	if st.Role != n.last.Role || st.Term != n.last.Term || st.Leader != n.last.Leader {
		n.logger.Info("raft state changed", "role", st.Role, "term", st.Term, "leader", st.Leader)
	}
	n.last = st
}
