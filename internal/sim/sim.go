package sim

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/hmzckd/quorumkv/internal/kv"
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
	SnapshotEvery  uint64
	UnsafeReads    bool
}

type Cluster struct {
	opts      Options
	rand      *rand.Rand
	now       int
	ids       []string
	nodes     map[string]*node
	inflight  []envelope
	seq       int
	group     map[string]int
	events    []string
	leaders   map[uint64]string
	committed map[uint64]raft.Entry
	commitIn  map[uint64]uint64
	digests   map[uint64]uint64
	failure   error

	clock   int64
	ops     []*clientOp
	history []HistoryOp
}

type node struct {
	id       string
	raft     *raft.Raft
	disk     disk
	up       bool
	restarts uint64
	status   raft.Status
	applied  uint64
	digest   uint64
	kv       *kv.Store
	reads    map[uint64]uint64
}

type disk struct {
	hardState raft.HardState
	snapshot  *raft.Snapshot
	entries   []raft.Entry
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
		opts:      opts,
		rand:      rand.New(rand.NewPCG(opts.Seed, 0)),
		nodes:     map[string]*node{},
		group:     map[string]int{},
		leaders:   map[uint64]string{},
		committed: map[uint64]raft.Entry{},
		commitIn:  map[uint64]uint64{},
		digests:   map[uint64]uint64{},
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
	}, raft.State{HardState: n.disk.hardState, Snapshot: n.disk.snapshot, Entries: n.disk.entries})
	if err != nil {
		panic(err)
	}
	n.raft = r
	n.up = true
	n.applied, n.digest, n.kv = 0, 0, kv.New()
	if snap := n.disk.snapshot; snap != nil {
		n.applied = snap.Index
		n.digest, n.kv = decodeState(snap.Data)
	}
	n.reads = map[uint64]uint64{}
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
	if rd.Snapshot != nil {
		c.installSnapshot(n, *rd.Snapshot)
	}
	if len(rd.Entries) > 0 {
		c.persistEntries(n, rd.Entries)
	}
	for _, m := range rd.Messages {
		c.send(m)
	}
	for _, e := range rd.Committed {
		c.apply(n, e)
	}
	if every := c.opts.SnapshotEvery; every > 0 && c.failure == nil && n.applied-n.raft.Status().SnapshotIndex >= every {
		snap, err := n.raft.Compact(n.applied, encodeState(n.digest, n.kv))
		if err != nil {
			c.fail("%s compact: %v", n.id, err)
			return
		}
		c.keepAfter(n, snap)
	}
	for _, rs := range rd.ReadStates {
		n.reads[rs.ID] = rs.Index
		c.eventf("%s confirmed read #%d at index %d", n.id, rs.ID, rs.Index)
	}
	c.serveReads(n)
}

func (c *Cluster) ReadIndex(id string) (uint64, error) {
	n := c.nodes[id]
	if !n.up {
		return 0, fmt.Errorf("%s is down", id)
	}
	readID, err := n.raft.ReadIndex()
	if err == nil {
		c.eventf("%s started read #%d, waiting for a majority to confirm it is still leader", id, readID)
	}
	c.handleReady(n)
	return readID, err
}

func (c *Cluster) ReadConfirmed(id string, readID uint64) (uint64, bool) {
	index, ok := c.nodes[id].reads[readID]
	return index, ok
}

func (c *Cluster) persistEntries(n *node, entries []raft.Entry) {
	first := entries[0].Index
	for i, e := range n.disk.entries {
		if e.Index == first {
			c.eventf("%s overwrote its log from index %d (had %d entries)", n.id, first, n.disk.entries[len(n.disk.entries)-1].Index)
			n.disk.entries = n.disk.entries[:i]
			break
		}
	}
	n.disk.entries = append(n.disk.entries, entries...)
}

func (c *Cluster) keepAfter(n *node, snap raft.Snapshot) {
	var keep []raft.Entry
	for i, e := range n.disk.entries {
		if e.Index == snap.Index && e.Term == snap.Term {
			keep = slices.Clone(n.disk.entries[i+1:])
		}
	}
	n.disk.snapshot = &snap
	n.disk.entries = keep
}

func (c *Cluster) installSnapshot(n *node, snap raft.Snapshot) {
	c.keepAfter(n, snap)
	n.applied = snap.Index
	n.digest, n.kv = decodeState(snap.Data)
	if want, ok := c.digests[snap.Index]; ok && want != n.digest {
		c.fail("%s installed a snapshot at index %d whose state differs from what other nodes applied", n.id, snap.Index)
	}
	c.eventf("%s installed snapshot up to index %d from the leader", n.id, snap.Index)
}

func nextDigest(prev uint64, e raft.Entry) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], prev)
	h.Write(buf[:])
	h.Write(e.Data)
	return h.Sum64()
}

func encodeState(digest uint64, store *kv.Store) []byte {
	return append(binary.BigEndian.AppendUint64(nil, digest), store.Snapshot()...)
}

func decodeState(b []byte) (uint64, *kv.Store) {
	store := kv.New()
	if len(b) < 8 {
		return 0, store
	}
	if err := store.Restore(b[8:]); err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(b[:8]), store
}

func (c *Cluster) apply(n *node, e raft.Entry) {
	if c.failure != nil {
		return
	}
	if e.Index != n.applied+1 {
		c.fail("%s applied index %d after %d: gap or repeat", n.id, e.Index, n.applied)
		return
	}
	n.applied = e.Index
	n.digest = nextDigest(n.digest, e)
	_ = n.kv.Apply(e.Data)
	c.onApplied(n, e)
	if want, ok := c.digests[e.Index]; ok && want != n.digest {
		c.fail("%s has a different state after index %d than another node", n.id, e.Index)
		return
	}
	c.digests[e.Index] = n.digest
	if prev, ok := c.committed[e.Index]; ok {
		if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
			c.fail("state machine safety violated: %s applied %s at index %d, another node applied %s",
				n.id, describe(e), e.Index, describe(prev))
		}
		return
	}
	c.committed[e.Index] = e
	c.commitIn[e.Index] = n.raft.Status().Term
	if len(e.Data) > 0 {
		c.eventf("%s committed index %d %s", n.id, e.Index, describe(e))
	}
}

func describe(e raft.Entry) string {
	if len(e.Data) == 0 {
		return fmt.Sprintf("(term %d, no-op)", e.Term)
	}
	return fmt.Sprintf("(term %d, %q)", e.Term, e.Data)
}

func (c *Cluster) Propose(id string, data string) (index, term uint64, err error) {
	n := c.nodes[id]
	if !n.up {
		return 0, 0, fmt.Errorf("%s is down", id)
	}
	index, term, err = n.raft.Propose([]byte(data))
	if err == nil {
		c.eventf("%s accepted %q at index %d term %d", id, data, index, term)
	}
	c.handleReady(n)
	return index, term, err
}

func (c *Cluster) Committed(index uint64) (raft.Entry, bool) {
	e, ok := c.committed[index]
	return e, ok
}

func (c *Cluster) Log(id string) []raft.Entry {
	return slices.Clone(c.nodes[id].disk.entries)
}

func (c *Cluster) fail(format string, args ...any) {
	if c.failure == nil {
		c.failure = fmt.Errorf("seed %d, tick %d: %s\n%s", c.opts.Seed, c.now, fmt.Sprintf(format, args...), c.Timeline())
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
	c.expireOps()
	if err := c.checkElectionSafety(); err != nil {
		c.fail("%v", err)
	}
	if err := c.checkLeaderCompleteness(); err != nil {
		c.fail("%v", err)
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

func (c *Cluster) checkLeaderCompleteness() error {
	for _, id := range c.Leaders() {
		r := c.nodes[id].raft
		term := c.nodes[id].status.Term
		snapIndex := r.Status().SnapshotIndex
		for index, e := range c.committed {
			if term <= c.commitIn[index] || index <= snapIndex {
				continue
			}
			if t, ok := r.LogTerm(index); !ok || t != e.Term {
				return fmt.Errorf("leader completeness violated: leader %s of term %d lacks index %d committed in term %d",
					id, term, index, c.commitIn[index])
			}
		}
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
	c.abandonOps(func(op *clientOp) bool { return op.node == id })
}

func (c *Cluster) Restart(id string) {
	n := c.nodes[id]
	n.restarts++
	c.start(n)
	var snapIndex uint64
	if n.disk.snapshot != nil {
		snapIndex = n.disk.snapshot.Index
	}
	c.eventf("restart %s (term=%d votedFor=%q, snapshot up to %d, %d log entries from disk)",
		id, n.disk.hardState.Term, n.disk.hardState.VotedFor, snapIndex, len(n.disk.entries))
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
