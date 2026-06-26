# VL.IO.Raft

Distributed leader election for [vvvv gamma](https://vvvv.org) using the [Raft consensus algorithm](https://raft.github.io/).

Built on [DotNext.Net.Cluster](https://dotnet.github.io/dotNext/features/cluster/raft.html) (v5.x, net8.0).

---

## Node: LeaderElection (IO.Raft)

Runs a Raft consensus cluster among a fixed set of known machines and surfaces the elected leader.

### Inputs

| Pin | Type | Default | Description |
|-----|------|---------|-------------|
| Hosts | Spread\<string\> | | IP addresses or hostnames of **all** machines in the cluster, in the same order on every machine |
| Port | int | 3262 | TCP port all machines listen on |
| LocalId | int | 0 | Index of **this** machine in the Hosts list. When Prioritize is enabled, lower index means higher election priority |
| Prioritize | bool | false | When true, scales election timeouts by LocalId so the node with the lowest index is most likely to win. When false, all nodes have equal chance (standard Raft behaviour) |
| Enable | bool | false | Set to true to start the cluster |

### Outputs

| Pin | Type | Description |
|-----|------|-------------|
| Leader | string | `host:port` of the current leader, empty string if no leader is known yet |
| IsLeader | bool | True if this machine is the current leader |
| HasLeader | bool | True if any leader is known |
| Error | string | Error message if the cluster failed to start |

### Usage

1. Add the `LeaderElection` node to your patch on every machine.
2. Connect the same `Hosts` spread to all instances (e.g. `["192.168.1.10", "192.168.1.11", "192.168.1.12"]`).
3. Set `LocalId` to this machine's index in that list (0 on the first machine, 1 on the second, etc.).
4. Set `Enable` to true.

All machines should converge on the same leader within ~1 second. If the current leader goes offline, the remaining nodes elect a new one automatically.

### Election Priority

Standard Raft uses randomised election timeouts to break ties — the node that times out first becomes a candidate and usually wins. With `Prioritize = false` (the default) all nodes use the same timeout range and have equal chance of winning.

When `Prioritize = true`, the timeout is scaled by `LocalId`:

```
LowerElectionTimeout = 150 + LocalId × 50 ms
UpperElectionTimeout = 300 + LocalId × 50 ms
```

Node 0 uses 150–300 ms, node 1 uses 200–350 ms, and so on. In a healthy cluster, node 0 will almost always win. If node 0 is offline, node 1 wins, and so on.

### Node Rejoin Behaviour

When a machine crashes and restarts, it reconnects to the cluster automatically. The current leader sends heartbeats to all known members; the rejoining node receives one within a heartbeat interval and updates its `Leader` and `IsLeader` outputs accordingly.

This works without any extra configuration because the static member list means all nodes always know each other's addresses.

One thing to be aware of: if the rejoining node was the previous leader, the remaining nodes will have already elected a replacement (provided a majority stayed online). The rejoining node becomes a follower under the new leader and **will not automatically reclaim leadership** — even if it has the lowest `LocalId`. It would only win a subsequent election if the current leader goes offline.

### Notes

- All machines must use the same `Hosts` list in the same order.
- The cluster uses a static membership list. All nodes start with `ColdStart = true` and immediately hold an election. Each node's member list is pre-seeded with all configured peers so that proper quorum (majority) is required — no node can win a solo election when others are reachable.
- Transport is TCP (no ASP.NET Core required). See [DotNext transport options](https://dotnet.github.io/dotNext/features/cluster/raft.html) for HTTP alternatives.
- This library uses `ConsensusOnlyState` (the DotNext default) — no data is replicated, only the leader identity is established.

### Quorum and fault tolerance

| Cluster size | Votes needed | Max simultaneous failures |
|---|---|---|
| 2 | 2 / 2 | 0 — losing either node = no quorum |
| 3 | 2 / 3 | 1 |
| 4 | 3 / 4 | 1 |
| 5 | 3 / 5 | 2 |
| N | ⌊N/2⌋ + 1 | ⌊N/2⌋ |

**2-node clusters**: a 2-node cluster requires both nodes to be online for any election to succeed. This is an inherent Raft property, not a library limitation. Consider a 3-node cluster for fault tolerance.

**Arbitrary startup order**: nodes that start early will fail their elections (cannot reach majority of a still-offline cluster) and keep retrying with randomised timeouts. Once a majority of configured nodes is online, an election succeeds automatically — no restart or manual intervention required.

---


## Getting started
- Install as [described here](https://thegraybook.vvvv.org/reference/hde/managing-nugets.html) via commandline:

    `nuget VL.IO.Raft`

- The near future: Install the `VL.IO.Raft` NuGet package via the vvvv package manager.


- Usage examples and more information are included in the pack and can be found via the [Help Browser](https://thegraybook.vvvv.org/reference/hde/findinghelp.html)



---

## Upgrading to DotNext 6.x

DotNext 6.x requires net10 and has an incompatible wire protocol. To upgrade:

1. Update `DotNext.Net.Cluster` to `6.*` in `src/VL.IO.Raft.csproj`.
2. Rebuild.
3. Update **all** cluster nodes simultaneously — a mixed 5.x/6.x cluster will not work.

---

## References

- [DotNext Raft overview](https://dotnet.github.io/dotNext/features/cluster/raft.html)
- [IRaftCluster API](https://dotnet.github.io/dotNext/api/DotNext.Net.Cluster.Consensus.Raft.IRaftCluster.html)
- [IClusterMember API](https://dotnet.github.io/dotNext/api/DotNext.Net.Cluster.IClusterMember.html)
- [Raft paper (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf)

---

## Credits

Initial development was sponsored by [Refik Anadol Studio](https://refikanadolstudio.com/).
