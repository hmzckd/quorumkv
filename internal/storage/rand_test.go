package storage

import "math/rand/v2"

func newRand() *rand.Rand { return rand.New(rand.NewPCG(1, 1)) }
