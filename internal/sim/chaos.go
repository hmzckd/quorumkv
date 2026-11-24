package sim

import (
	"slices"
)

func (c *Cluster) Chaos(ticks int, faultRate float64, each func(tick int)) error {
	for i := 0; i < ticks && c.failure == nil; i++ {
		if c.rand.Float64() < faultRate {
			c.randomFault()
		}
		if each != nil {
			each(i)
		}
		c.step()
	}
	return c.failure
}

func (c *Cluster) Settle(ticks int) error {
	c.opts.DropRate = 0
	c.opts.MaxDelay = c.opts.MinDelay + 2
	c.Heal()
	for _, id := range c.ids {
		if !c.nodes[id].up {
			c.Restart(id)
		}
	}
	return c.Run(ticks)
}

func (c *Cluster) randomFault() {
	var up, down []string
	for _, id := range c.ids {
		if c.nodes[id].up {
			up = append(up, id)
		} else {
			down = append(down, id)
		}
	}

	switch c.rand.IntN(4) {
	case 0:
		if len(up) > 0 {
			c.Crash(up[c.rand.IntN(len(up))])
		}
	case 1:
		if len(down) > 0 {
			c.Restart(down[c.rand.IntN(len(down))])
		}
	case 2:
		ids := slices.Clone(c.ids)
		c.rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		cut := 1 + c.rand.IntN(len(ids)-1)
		c.Partition(ids[:cut], ids[cut:])
	case 3:
		c.Heal()
	}
}

func (c *Cluster) RandomNode() string {
	return c.ids[c.rand.IntN(len(c.ids))]
}

func (c *Cluster) Chance(p float64) bool {
	return c.rand.Float64() < p
}
