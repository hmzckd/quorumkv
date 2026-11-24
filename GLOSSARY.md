# Glossary

Terms used in the code and the docs. Each term means exactly one thing.

| Term | Meaning |
|---|---|
| **Node** | One running copy of the `quorumkv` program, with its own data directory. Identified by a short id such as `n1`. |
| **Cluster** | The fixed set of nodes that together act as one key-value store. Membership does not change while the cluster runs. |
| **Peer** | Any node of the cluster, as listed in `QK_PEERS`. A node's peer list includes the node itself. |
| **Quorum** | The smallest majority of the cluster: `size/2 + 1`. A 3-node cluster has a quorum of 2 and keeps working with one node down. |
| **Term** | A logical clock that only grows. Each election starts a new term; at most one leader exists per term. A node that sees a higher term adopts it and becomes a follower. |
| **Leader** | The one node of a term that accepts writes and sends heartbeats. |
| **Follower** | A node that accepts the leader's messages and votes in elections. Every node starts as a follower. |
| **Candidate** | A follower whose election timeout fired. It increments the term, votes for itself and asks the others for votes. |
| **Election timeout** | How long a follower waits without hearing from a leader before it becomes a candidate. Randomized between 1x and 2x the configured value so that candidates rarely collide. |
| **Heartbeat** | An empty `AppendEntries` the leader sends regularly to say "I am still here". |
| **Hard state** | The term and the vote of a node. It must reach disk before the node answers, otherwise a restarted node could vote twice in one term. |
| **Tick** | One unit of time for the Raft core. The core never reads the clock; the runtime calls `Tick()` every 10 ms. |
| **Ready** | What the core asks the runtime to do after an input: state to persist and messages to send. |
| **Simulation** | Tests in `internal/sim` that run several Raft cores in one process over a fake network and a fake clock. The test decides which messages are lost, delayed or blocked. |
| **Seed** | The number that drives every random choice of a simulation run. The same seed always replays the same run. |
| **Partition** | A network split: nodes in different groups cannot reach each other until the network heals. |
| **Election safety** | At most one leader per term. The simulator checks it after every tick. |
| **Log** | The ordered list of entries a node keeps. Index 1 is the first entry. The leader's log is the source of truth; followers copy it. |
| **Entry** | One command in the log, with the term it was created in and its index. An entry with no data is a no-op. |
| **No-op entry** | An empty entry a new leader appends right after winning. Committing it also commits every older entry and proves the leader's log is current. |
| **Commit** | An entry is committed once the leader of its term has it stored on a quorum. A committed entry is never lost or changed. |
| **Commit index** | The highest index a node knows to be committed. |
| **Apply** | Executing committed entries, in index order, against the key-value map. Every node applies the same entries in the same order. |
| **Log matching** | If two logs have an entry with the same index and term, the logs are identical up to that index. `AppendEntries` checks the entry before the new ones (`prevLogIndex`, `prevLogTerm`) to keep this true. |
| **Leader completeness** | A leader always holds every entry committed in earlier terms. Votes go only to candidates whose log is at least as up to date as the voter's. |
| **State machine safety** | No two nodes ever apply different entries at the same index. The simulator checks it on every apply. |
| **fsync** | Asking the operating system to push a file's data to the physical disk. Until then a power cut can lose it. |
| **Torn write** | A record that was only partly written when the process or machine died. The log reader finds it by its length or checksum and cuts it off. |
| **CRC** | A 32-bit checksum stored with each log record. A single changed byte makes it mismatch. |
| **Transport** | Carries Raft messages between nodes. Every message is a one-way `POST /raft/message` with a JSON body; a reply is a separate message going the other way. |
| **Node loop** | The single goroutine in `internal/node` that feeds ticks and messages to the Raft core, writes `Ready` to disk, then hands its messages to the transport. Only this goroutine touches the core, so the core needs no locks. |
| **Command** | What a log entry carries for the key-value store: `{"op":"put","key":"x","value":"1"}` or `{"op":"delete","key":"x"}`. |
| **Idempotent** | Running a command twice leaves the same result as running it once. `PUT` and `DELETE` are idempotent, so a client may safely retry them. |
| **Outcome unknown** | The answer to a write whose fate the leader cannot know (timeout, or it lost leadership). The entry may still be committed later. The client retries. |
| **Redirect** | A follower answers client requests with `307 Temporary Redirect` and the leader's address in `Location`. |
| **Linearizable read** | A read that returns the value of the latest write acknowledged before the read started, as if there were one copy of the data. |
| **Stale read** | A read that returns an older value. A leader cut off in a minority would give stale reads if it answered from its own map. |
| **ReadIndex** | How the leader serves linearizable reads: note the commit index, confirm leadership with a majority through one heartbeat round, wait until that index is applied, then read. |
| **Snapshot** | The whole key-value map as of one log index, with that entry's term. Stored in the `snapshot` file. |
| **Log compaction** | After applying `QK_SNAPSHOT_EVERY` entries, a node writes a snapshot and deletes the log entries it covers, so the log and the restart time stay small. |
| **InstallSnapshot** | The message a leader sends instead of `AppendEntries` when a follower needs entries the leader already compacted away. The follower replaces its map with the snapshot. |
