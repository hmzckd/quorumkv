package sim

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"testing"
)

var (
	seedFlag  = flag.Uint64("seed", 0, "run only this simulation seed")
	seedsFlag = flag.Int("seeds", 200, "how many seeds TestRandomFaults runs")
)

func seeds(t *testing.T) []uint64 {
	if *seedFlag != 0 {
		return []uint64{*seedFlag}
	}
	n := *seedsFlag
	if testing.Short() {
		n = 20
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i + 1)
	}
	return out
}

func randomOptions(seed uint64) Options {
	r := rand.New(rand.NewPCG(seed, 99))
	return Options{
		Seed:          seed,
		Nodes:         []int{3, 5}[r.IntN(2)],
		DropRate:      r.Float64() * 0.3,
		MaxDelay:      1 + r.IntN(10),
		SnapshotEvery: []uint64{0, 5, 20}[r.IntN(3)],
	}
}

func TestRandomFaults(t *testing.T) {
	for _, seed := range seeds(t) {
		opts := randomOptions(seed)
		if err := runRandomFaults(opts); err != nil {
			t.Fatalf("%v\n\nreplay: go test ./internal/sim -run TestRandomFaults -seed=%d -v", err, seed)
		}
		if *seedFlag != 0 {
			t.Logf("seed %d (%d nodes, drop %.0f%%, delay up to %d, snapshot every %d) passed",
				seed, opts.Nodes, opts.DropRate*100, opts.MaxDelay, opts.SnapshotEvery)
		}
	}
}

func runRandomFaults(opts Options) error {
	c := New(opts)
	proposals := 0

	err := c.Chaos(3000, 0.01, func(tick int) {
		if c.Chance(0.1) {
			id := c.RandomNode()
			if c.IsUp(id) {
				if _, _, err := c.Propose(id, fmt.Sprintf("w%d", tick)); err == nil {
					proposals++
				}
			}
		}
		if c.Chance(0.05) {
			if id := c.RandomNode(); c.IsUp(id) {
				c.ReadIndex(id)
			}
		}
	})
	if err != nil {
		return err
	}

	if err := c.Settle(1000); err != nil {
		return err
	}
	leader, _ := c.LeaderOfHighestTerm()
	if leader == "" {
		return fmt.Errorf("seed %d: no leader 1000 ticks after healing everything\n%s", opts.Seed, c.Timeline())
	}
	index, _, err := c.Propose(leader, "final")
	if err != nil {
		return fmt.Errorf("seed %d: final proposal: %v", opts.Seed, err)
	}
	if err := c.RunUntil(1000, func() bool {
		for _, id := range c.IDs() {
			if c.nodes[id].applied < index {
				return false
			}
		}
		return true
	}); err != nil {
		return fmt.Errorf("seed %d: the cluster did not recover: %v\n%s", opts.Seed, err, c.Timeline())
	}
	return nil
}
