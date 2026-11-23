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
