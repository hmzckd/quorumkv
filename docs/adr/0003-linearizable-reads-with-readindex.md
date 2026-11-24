# 3. Linearizable reads with ReadIndex

Date: 2026-11-24

## Context

A leader cut off in a minority does not know it has been replaced (no message can reach it). If it answered reads
from its own map, a client could read `x=1` after another client's `x=2` was acknowledged by the new leader. Reads
must see every write acknowledged before the read started.

Options:

- **Read through the log:** append each read as an entry. Correct, but every read costs a disk write and a full
  replication round.
- **Leader lease:** the leader serves reads alone while it believes no new leader can exist yet. Fast, but its
  safety depends on bounded clock drift between machines.
- **ReadIndex:** the leader notes its commit index, confirms with one heartbeat round that a majority still follows
  it, waits until that index is applied, then reads locally.

## Decision

Use ReadIndex, as etcd does.

- A leader only serves reads after it has committed an entry of its own term (its no-op), so its commit index
  includes everything committed by earlier leaders. Before that it answers `503 Leader not ready`.
- Every `AppendEntries` carries the newest read id (`readContext`); followers echo it. A read is confirmed when a
  majority (counting the leader) has echoed an id at least as large as its own. A reply to a heartbeat sent before
  the read started cannot confirm it.
- Followers redirect reads to the leader. Follower reads are left out.

## Consequences

- Reads cost one network round trip to a majority but no disk write.
- A leader without a majority answers reads with `503`, never with stale data.
- No dependence on clocks.
