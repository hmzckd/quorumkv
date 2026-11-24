package sim

import (
	"math"

	"github.com/hmzckd/quorumkv/internal/kv"
	"github.com/hmzckd/quorumkv/internal/raft"
)

const opTimeoutTicks = 300

type OpInput struct {
	Kind  string
	Key   string
	Value string
}

type OpOutput struct {
	Value   string
	Found   bool
	Unknown bool
}

type HistoryOp struct {
	Client int
	Input  OpInput
	Output OpOutput
	Call   int64
	Return int64
}

type clientOp struct {
	client  int
	input   OpInput
	call    int64
	started int
	node    string
	term    uint64
	index   uint64
	readID  uint64
}

func (c *Cluster) tickClock() int64 {
	c.clock++
	return c.clock
}

func (c *Cluster) ClientBusy(client int) bool {
	for _, op := range c.ops {
		if op.client == client {
			return true
		}
	}
	return false
}

func (c *Cluster) ClientTarget() string {
	leaders := c.Leaders()
	if len(leaders) > 0 {
		return leaders[c.rand.IntN(len(leaders))]
	}
	return c.RandomNode()
}

func (c *Cluster) StartPut(client int, node, key, value string) {
	n := c.nodes[node]
	if !n.up {
		return
	}
	op := &clientOp{client: client, input: OpInput{Kind: "put", Key: key, Value: value}, started: c.now, node: node}
	op.call = c.tickClock()
	index, term, err := n.raft.Propose(kv.Command{Op: kv.OpPut, Key: key, Value: value}.Encode())
	if err != nil {
		return
	}
	op.index, op.term = index, term
	c.ops = append(c.ops, op)
	c.handleReady(n)
}

func (c *Cluster) StartGet(client int, node, key string) {
	n := c.nodes[node]
	if !n.up {
		return
	}
	op := &clientOp{client: client, input: OpInput{Kind: "get", Key: key}, started: c.now, node: node}
	op.call = c.tickClock()

	if c.opts.UnsafeReads {
		if n.raft.Status().Role != raft.Leader {
			return
		}
		value, found := n.kv.Get(key)
		c.finish(op, OpOutput{Value: value, Found: found}, c.tickClock())
		return
	}

	readID, err := n.raft.ReadIndex()
	if err != nil {
		return
	}
	op.readID = readID
	c.ops = append(c.ops, op)
	c.handleReady(n)
}

func (c *Cluster) History() []HistoryOp {
	return c.history
}

func (c *Cluster) onApplied(n *node, e raft.Entry) {
	c.completeOps(func(op *clientOp) (bool, OpOutput) {
		if op.node != n.id || op.input.Kind != "put" || op.index != e.Index {
			return false, OpOutput{}
		}
		return true, OpOutput{Unknown: op.term != e.Term}
	})
}

func (c *Cluster) serveReads(n *node) {
	c.completeOps(func(op *clientOp) (bool, OpOutput) {
		if op.node != n.id || op.input.Kind != "get" {
			return false, OpOutput{}
		}
		index, ok := n.reads[op.readID]
		if !ok || index > n.applied {
			return false, OpOutput{}
		}
		value, found := n.kv.Get(op.input.Key)
		return true, OpOutput{Value: value, Found: found}
	})
}

func (c *Cluster) expireOps() {
	c.abandonOps(func(op *clientOp) bool {
		n := c.nodes[op.node]
		lost := !n.up || n.status.Role != raft.Leader || (op.term != 0 && n.status.Term != op.term)
		return lost || c.now-op.started > opTimeoutTicks
	})
}

func (c *Cluster) abandonOps(match func(*clientOp) bool) {
	c.completeOps(func(op *clientOp) (bool, OpOutput) {
		if !match(op) {
			return false, OpOutput{}
		}
		return true, OpOutput{Unknown: true}
	})
}

func (c *Cluster) completeOps(decide func(*clientOp) (bool, OpOutput)) {
	remaining := c.ops[:0]
	for _, op := range c.ops {
		done, out := decide(op)
		if !done {
			remaining = append(remaining, op)
			continue
		}
		switch {
		case out.Unknown && op.input.Kind == "get":
		case out.Unknown:
			c.finish(op, out, math.MaxInt64)
		default:
			c.finish(op, out, c.tickClock())
		}
	}
	c.ops = remaining
}

func (c *Cluster) finish(op *clientOp, out OpOutput, ret int64) {
	c.history = append(c.history, HistoryOp{Client: op.client, Input: op.input, Output: out, Call: op.call, Return: ret})
}
