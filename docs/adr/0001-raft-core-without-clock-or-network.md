# 1. The Raft core knows no clock and no network

Date: 2026-11-23

## Status

Accepted

## Context

Raft bugs appear only under specific orderings of timeouts, lost messages and crashes. If the algorithm reads the
real clock and talks to the network itself, a failing ordering happens once and cannot be repeated.

## Decision

`internal/raft` is a plain state machine with two inputs: `Tick()` (one unit of time passed) and `Step(msg)`
(a message arrived). It never sleeps, starts goroutines, takes locks or does I/O. After each input the caller takes
`Ready()`: the state to persist and the messages to send. A thin runtime connects it to a real ticker, HTTP and
disk. Randomness comes from a `*rand.Rand` passed in, so a seed fixes every election timeout.

## Consequences

- Tests and the simulator drive the core directly and decide exactly which message is delivered, delayed or lost.
- Any failure can be replayed from its seed.
- The caller must follow the contract: persist the hard state before sending the messages of the same `Ready`.
- This is the same split as etcd's raft package.
