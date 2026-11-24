package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

type kvState struct {
	value string
	found bool
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			key := op.Input.(OpInput).Key
			if _, ok := byKey[key]; !ok {
				keys = append(keys, key)
			}
			byKey[key] = append(byKey[key], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, key := range keys {
			out = append(out, byKey[key])
		}
		return out
	},
	Init: func() any { return kvState{} },
	Step: func(state, input, output any) (bool, any) {
		s := state.(kvState)
		in := input.(OpInput)
		out := output.(OpOutput)
		switch in.Kind {
		case "put":
			return true, kvState{value: in.Value, found: true}
		case "get":
			return out.Found == s.found && out.Value == s.value, s
		}
		return false, s
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(OpInput), output.(OpOutput)
		if in.Kind == "put" {
			return fmt.Sprintf("put(%s, %s)", in.Key, in.Value)
		}
		if !out.Found {
			return fmt.Sprintf("get(%s) -> not found", in.Key)
		}
		return fmt.Sprintf("get(%s) -> %s", in.Key, out.Value)
	},
}

func linearizabilitySeeds(t *testing.T) []uint64 {
	if *seedFlag != 0 {
		return []uint64{*seedFlag}
	}
	n := 30
	if testing.Short() {
		n = 5
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i + 1)
	}
	return out
}

func runClients(opts Options) ([]porcupine.Operation, error) {
	const clients = 5
	keys := []string{"k0", "k1", "k2"}
	c := New(opts)
	written := 0

	err := c.Chaos(2000, 0.005, func(tick int) {
		for client := range clients {
			if c.ClientBusy(client) || !c.Chance(0.2) {
				continue
			}
			node := c.ClientTarget()
			key := keys[c.rand.IntN(len(keys))]
			if c.Chance(0.5) {
				written++
				c.StartPut(client, node, key, fmt.Sprintf("v%d", written))
			} else {
				c.StartGet(client, node, key)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	if err := c.Settle(300); err != nil {
		return nil, err
	}

	var history []porcupine.Operation
	for _, op := range c.History() {
		history = append(history, porcupine.Operation{
			ClientId: op.Client,
			Input:    op.Input,
			Output:   op.Output,
			Call:     op.Call,
			Return:   op.Return,
		})
	}
	return history, nil
}

func TestLinearizableUnderFaults(t *testing.T) {
	for _, seed := range linearizabilitySeeds(t) {
		opts := randomOptions(seed)
		history, err := runClients(opts)
		if err != nil {
			t.Fatalf("%v\n\nreplay: go test ./internal/sim -run TestLinearizableUnderFaults -seed=%d -v", err, seed)
		}
		result := porcupine.CheckOperationsTimeout(kvModel, history, 10*time.Second)
		if result == porcupine.Illegal {
			t.Fatalf("seed %d: history of %d operations is not linearizable\n\nreplay: go test ./internal/sim -run TestLinearizableUnderFaults -seed=%d -v",
				seed, len(history), seed)
		}
		if *seedFlag != 0 || result == porcupine.Unknown {
			t.Logf("seed %d: %d operations, result %s", seed, len(history), result)
		}
	}
}

func TestStaleReadsAreCaught(t *testing.T) {
	var caught []uint64
	for _, seed := range linearizabilitySeeds(t) {
		opts := randomOptions(seed)
		opts.UnsafeReads = true
		history, err := runClients(opts)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if porcupine.CheckOperationsTimeout(kvModel, history, 10*time.Second) == porcupine.Illegal {
			caught = append(caught, seed)
		}
	}
	if len(caught) == 0 {
		t.Fatal("reading from the local state without ReadIndex should produce a stale read on some seed")
	}
	t.Logf("stale reads caught on seeds %v", caught)
}
