package sim

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/hmzckd/quorumkv/internal/raft"
)

type Options struct {
	Seed           uint64
	Nodes          int
	DropRate       float64
	MinDelay       int
	MaxDelay       int
	ElectionTicks  int
	HeartbeatTicks int
}

type Cluster struct {
	opts     Options
	rand     *rand.Rand
	now      int
	ids      []string
	nodes    map[string]*node
	inflight []envelope
	seq      int
	group    map[string]int
	events   []string
	leaders  map[uint64]string
	failure  error
}

type node struct {
	id       string
	raft     *raft.Raft
	disk     disk
	up       bool
	restarts uint64
	status   raft.Status
}

type disk struct {
	hardState raft.HardState
}

type envelope struct {
	deliverAt int
	seq       int
	msg       raft.Message
}

func New(opts Options) *Cluster {
	if opts.Nodes == 0 {
		opts.Nodes = 3
	}
	if opts.MinDelay == 0 {
		opts.MinDelay = 1
	}
	if opts.MaxDelay < opts.MinDelay {
		opts.MaxDelay = opts.MinDelay + 2
	}
	if opts.ElectionTicks == 0 {
		opts.ElectionTicks = 15
	}
	if opts.HeartbeatTicks == 0 {
		opts.HeartbeatTicks = 5
	}

	c := &Cluster{
		opts:    opts,
		rand:    rand.New(rand.NewPCG(opts.Seed, 0)),
		nodes:   map[string]*node{},
		group:   map[string]int{},
		leaders: map[uint64]string{},
	}
	for i := 1; i <= opts.Nodes; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		n := &node{id: id}
		c.nodes[id] = n
		c.start(n)
	}
	return c
}

func (c *Cluster) start(n *node) {
	r, err := raft.New(raft.Config{
		ID:             n.id,
		Peers:          c.ids,
		ElectionTicks:  c.opts.ElectionTicks,
		HeartbeatTicks: c.opts.HeartbeatTicks,
		Rand:           rand.New(rand.NewPCG(c.opts.Seed, uint64(slices.Index(c.ids, n.id)+1)<<16|n.restarts)),
	}, n.disk.hardState)
	if err != nil {
		panic(err)
	}
	n.raft = r
	n.up = true
	n.status = r.Status()
}

func (c *Cluster) IDs() []string { return slices.Clone(c.ids) }

func (c *Cluster) Now() int { return c.now }

func (c *Cluster) Run(ticks int) error {
	for i := 0; i < ticks && c.failure == nil; i++ {
		c.step()
	}
	return c.failure
}

func (c *Cluster) RunUntil(maxTicks int, cond func() bool) error {
	for i := 0; i < maxTicks && c.failure == nil; i++ {
		if cond() {
			return nil
		}
		c.step()
	}
	if c.failure != nil {
		return c.failure
	}
	if cond() {
		return nil
	}
	return fmt.Errorf("condition not reached within %d ticks", maxTicks)
}

func (c *Cluster) step() {
	c.now++

	var due, later []envelope
	for _, e := range c.inflight {
		if e.deliverAt <= c.now {
			due = append(due, e)
		} else {
			later = append(later, e)
		}
	}
	c.inflight = later
	slices.SortFunc(due, func(a, b envelope) int {
		if a.deliverAt != b.deliverAt {
			return a.deliverAt - b.deliverAt
		}
		return a.seq - b.seq
	})
	for _, e := range due {
		to := c.nodes[e.msg.To]
		if !to.up || !c.connected(e.msg.From, e.msg.To) {
			continue
		}
		to.raft.Step(e.msg)
		c.handleReady(to)
	}

	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.up {
			continue
		}
		n.raft.Tick()
		c.handleReady(n)
	}

	c.observe()
}

func (c *Cluster) handleReady(n *node) {
	rd := n.raft.Ready()
	if rd.HardState != nil {
		n.disk.hardState = *rd.HardState
	}
	for _, m := range rd.Messages {
		c.send(m)
	}
}

func (c *Cluster) send(m raft.Message) {
	if !c.connected(m.From, m.To) {
		return
	}
	if c.opts.DropRate > 0 && c.rand.Float64() < c.opts.DropRate {
		return
	}
	delay := c.opts.MinDelay + c.rand.IntN(c.opts.MaxDelay-c.opts.MinDelay+1)
	c.seq++
	c.inflight = append(c.inflight, envelope{deliverAt: c.now + delay, seq: c.seq, msg: m})
}

func (c *Cluster) observe() {
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.up {
			continue
		}
		st := n.raft.Status()
		if st.Role != n.status.Role || st.Term != n.status.Term || st.Leader != n.status.Leader {
			c.eventf("%s %s term=%d leader=%q", id, st.Role, st.Term, st.Leader)
		}
		n.status = st
	}
	if err := c.checkElectionSafety(); err != nil && c.failure == nil {
		c.failure = fmt.Errorf("seed %d, tick %d: %w\n%s", c.opts.Seed, c.now, err, c.Timeline())
	}
}

func (c *Cluster) checkElectionSafety() error {
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.up || n.status.Role != raft.Leader {
			continue
		}
		if prev, ok := c.leaders[n.status.Term]; ok && prev != id {
			return fmt.Errorf("election safety violated: %s and %s are both leaders of term %d", prev, id, n.status.Term)
		}
		c.leaders[n.status.Term] = id
	}
	return nil
}

func (c *Cluster) connected(a, b string) bool {
	return c.group[a] == c.group[b]
}

func (c *Cluster) Partition(groups ...[]string) {
	c.group = map[string]int{}
	var parts []string
	for i, g := range groups {
		for _, id := range g {
			c.group[id] = i + 1
		}
		parts = append(parts, "["+strings.Join(g, " ")+"]")
	}
	c.eventf("partition %s", strings.Join(parts, " | "))
}

func (c *Cluster) Heal() {
	c.group = map[string]int{}
	c.eventf("heal network")
}

func (c *Cluster) Crash(id string) {
	n := c.nodes[id]
	n.up = false
	n.raft = nil
	c.eventf("crash %s", id)
}

func (c *Cluster) Restart(id string) {
	n := c.nodes[id]
	n.restarts++
	c.start(n)
	c.eventf("restart %s (term=%d votedFor=%q from disk)", id, n.disk.hardState.Term, n.disk.hardState.VotedFor)
}

func (c *Cluster) Status(id string) raft.Status {
	return c.nodes[id].status
}

func (c *Cluster) IsUp(id string) bool {
	return c.nodes[id].up
}

func (c *Cluster) Leaders() []string {
	var out []string
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.up && n.status.Role == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

func (c *Cluster) LeaderOfHighestTerm() (string, uint64) {
	var best string
	var term uint64
	for _, id := range c.Leaders() {
		if t := c.nodes[id].status.Term; best == "" || t > term {
			best, term = id, t
		}
	}
	return best, term
}

func (c *Cluster) HasLeaderIn(ids ...string) func() bool {
	return func() bool {
		for _, id := range ids {
			if c.nodes[id].up && c.nodes[id].status.Role == raft.Leader {
				return true
			}
		}
		return false
	}
}

func (c *Cluster) eventf(format string, args ...any) {
	c.events = append(c.events, fmt.Sprintf("t=%5d  %s", c.now, fmt.Sprintf(format, args...)))
}

func (c *Cluster) Events() []string { return slices.Clone(c.events) }

func (c *Cluster) Timeline() string {
	events := c.events
	if len(events) > 60 {
		events = events[len(events)-60:]
	}
	return strings.Join(events, "\n")
}
