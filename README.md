# VL.IO.Raft

A [vvvv gamma](https://vvvv.org) library for distributed leader election using the [Raft consensus algorithm](https://raft.github.io/).

Built on [DotNext.Net.Cluster](https://dotnet.github.io/dotNext/features/cluster/raft.html) (v5.x, net8.0).

---

## Node: LeaderElection (IO.Raft)

Runs a Raft consensus cluster among a fixed set of known machines and surfaces the elected leader.

### Inputs

| Pin | Type | Default | Description |
|-----|------|---------|-------------|
| Hosts | Spread\<string\> | | IP addresses or hostnames of **all** machines in the cluster, in the same order on every machine |
| Port | int | 3262 | TCP port all machines listen on |
| LocalId | int | 0 | Index of **this** machine in the Hosts list. Also controls election priority: lower index = shorter election timeout = more likely to become leader |
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

Standard Raft uses randomised election timeouts to break ties — the node that times out first becomes a candidate and usually wins. This library scales the timeout with `LocalId`:

```
LowerElectionTimeout = 150 + LocalId × 50 ms
UpperElectionTimeout = 300 + LocalId × 50 ms
```

Node 0 uses 150–300 ms, node 1 uses 200–350 ms, and so on. In a healthy cluster, node 0 will almost always win. If node 0 is offline, node 1 wins, and so on.

### Notes

- All machines must use the same `Hosts` list in the same order.
- The cluster uses a static membership list (`ColdStart = true`). Nodes do not need to discover each other; they connect directly using the provided addresses. See [DotNext cold start docs](https://dotnet.github.io/dotNext/features/cluster/raft.html) for details on the alternative dynamic membership model.
- Transport is TCP (no ASP.NET Core required). See [DotNext transport options](https://dotnet.github.io/dotNext/features/cluster/raft.html) for HTTP alternatives.
- This library uses `ConsensusOnlyState` (the DotNext default) — no data is replicated, only the leader identity is established.

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
