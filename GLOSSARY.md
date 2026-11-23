# Glossary

Terms used in the code and the docs. Each term means exactly one thing.

| Term | Meaning |
|---|---|
| **Node** | One running copy of the `quorumkv` program, with its own data directory. Identified by a short id such as `n1`. |
| **Cluster** | The fixed set of nodes that together act as one key-value store. Membership does not change while the cluster runs. |
| **Peer** | Any node of the cluster, as listed in `QK_PEERS`. A node's peer list includes the node itself. |
| **Quorum** | The smallest majority of the cluster: `size/2 + 1`. A 3-node cluster has a quorum of 2 and keeps working with one node down. |
