using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using System.Net;

namespace VL.IO.Raft;

// Pre-seeds the proposed member set with all N-1 non-self peers before RaftCluster.StartAsync.
// DotNext's startup sequence calls G.AddMemberAsync(self) then Ng.ApplyAsync().
// Our ApplyAsync fires ActiveConfigurationChanged for all N members so the cluster starts
// with full quorum knowledge instead of the default single-node view.
sealed class StaticConfigStorage : IClusterConfigurationStorage<EndPoint>, IClusterConfigurationStorage, IDisposable
{
    private readonly HashSet<EndPoint> _proposed;
    private readonly HashSet<EndPoint> _active = new();
    private event Func<EndPoint, bool, CancellationToken, ValueTask>? _changed;
    private long _activeFp = 0L;   // advances to 1L after the first ApplyAsync
    private bool _hasPending = true; // cleared after the first ApplyAsync

    internal StaticConfigStorage(IEnumerable<EndPoint> nonSelfPeers)
        => _proposed = new HashSet<EndPoint>(nonSelfPeers);

    // DotNext calls this during StartAsync to add the local node
    ValueTask<bool> IClusterConfigurationStorage<EndPoint>.AddMemberAsync(EndPoint ep, CancellationToken ct)
        => ValueTask.FromResult(_proposed.Add(ep));

    ValueTask<bool> IClusterConfigurationStorage<EndPoint>.RemoveMemberAsync(EndPoint ep, CancellationToken ct)
        => ValueTask.FromResult(_proposed.Remove(ep));

    IReadOnlySet<EndPoint> IClusterConfigurationStorage<EndPoint>.ActiveConfiguration => _active;
    IReadOnlySet<EndPoint>? IClusterConfigurationStorage<EndPoint>.ProposedConfiguration
        => _proposed.Count > 0 ? _proposed : null;

    event Func<EndPoint, bool, CancellationToken, ValueTask>? IClusterConfigurationStorage<EndPoint>.ActiveConfigurationChanged
    {
        add => _changed += value;
        remove => _changed -= value;
    }

    // DotNext calls this after AddMemberAsync(self) — fire event for every member in proposed
    async ValueTask IClusterConfigurationStorage.ApplyAsync(CancellationToken ct)
    {
        var toAdd = _proposed.Except(_active).ToList();
        var toRemove = _active.Except(_proposed).ToList();
        foreach (var ep in toRemove) { _active.Remove(ep); if (_changed is { } h) await h(ep, false, ct); }
        foreach (var ep in toAdd)    { _active.Add(ep);    if (_changed is { } h) await h(ep, true,  ct); }
        _activeFp = 1L;
        _hasPending = false;
    }

    // After the initial apply there is no pending configuration change.
    // Returning null for ProposedConfiguration tells DotNext not to drive joint-consensus replication.
    IClusterConfiguration IClusterConfigurationStorage.ActiveConfiguration   => new StubConfig(_activeFp);
    IClusterConfiguration? IClusterConfigurationStorage.ProposedConfiguration => _hasPending ? new StubConfig(1L) : null;

    ValueTask IClusterConfigurationStorage.LoadConfigurationAsync(CancellationToken ct) => ValueTask.CompletedTask;
    ValueTask IClusterConfigurationStorage.ProposeAsync(IClusterConfiguration cfg, CancellationToken ct) => ValueTask.CompletedTask;
    Task IClusterConfigurationStorage.WaitForApplyAsync(CancellationToken ct) => Task.CompletedTask;

    public void Dispose() { }

    sealed class StubConfig : IClusterConfiguration
    {
        internal StubConfig(long fingerprint) => Fingerprint = fingerprint;
        public long Fingerprint { get; }
        public long Length => 0L;
        bool IDataTransferObject.IsReusable => true;
        ValueTask IDataTransferObject.WriteToAsync<TWriter>(TWriter writer, CancellationToken token)
            => ValueTask.CompletedTask;
    }
}
