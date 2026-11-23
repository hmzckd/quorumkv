# 2. Own append-only log file with length, CRC and fsync

Date: 2026-11-23

## Status

Accepted

## Context

Raft is only safe if a node never forgets its term, its vote or an entry it acknowledged. An embedded database
(bbolt, BadgerDB) would give that, but it would hide the part this project is about: what has to reach disk, when,
and what a crash in the middle of a write leaves behind.

## Decision

Each node keeps two files in its data directory:

- `state`: the term and the vote as JSON. Written to `state.tmp`, fsynced, then renamed over `state`, so a crash
  leaves either the old or the new file, never half of one.
- `log`: append-only records `[length uint32][CRC32 uint32][JSON entry]`. Every append is fsynced before the node
  sends the messages of the same `Ready`. On start the file is read from the beginning; at the first record that is
  incomplete, fails its checksum or breaks the index order, the file is cut. A follower that must replace
  conflicting entries cuts the file at the byte offset of the first conflicting record.

`cmd/qkdump` prints both files without changing them.

## Consequences

- A record that was being written during a crash disappears. That is safe: it was not fsynced, so the node never
  acknowledged it.
- One fsync per `Ready` limits write throughput. Batching several client writes into one `Ready` is the usual fix
  and is left out on purpose.
- The whole log is read on start. Snapshots (QK-09) keep it short.
