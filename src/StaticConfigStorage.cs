using DotNext.Net.Cluster.Consensus.Raft.Membership;
using System.Net;

namespace VL.IO.Raft;

// Wraps InMemoryClusterConfigurationStorage and exposes all N-1 non-self peer endpoints
// via the typed ProposedConfiguration property — the path DotNext reads on startup to
// build its internal members list. ActiveConfiguration starts empty; binary/Raft-internal
// operations (LoadConfigurationAsync, ProposeAsync, ApplyAsync) are delegated to the inner
// InMemory storage so DotNext can handle any subsequent membership commits correctly.
sealed class StaticConfigStorage
    : IClusterConfigurationStorage<EndPoint>,
      IClusterConfigurationStorage,
      IDisposable
{
    private readonly IClusterConfigurationStorage<EndPoint> _innerGeneric;
    private readonly IClusterConfigurationStorage _innerNonGeneric;
    private readonly IDisposable _innerDisposable;
    private readonly HashSet<EndPoint> _peers;

    internal StaticConfigStorage(IClusterConfigurationStorage<EndPoint> inner, IEnumerable<EndPoint> peers)
    {
        _innerGeneric = inner;
        _innerNonGeneric = (IClusterConfigurationStorage)inner;
        _innerDisposable = (IDisposable)inner;
        _peers = new HashSet<EndPoint>(peers);
    }

    // ---- IClusterConfigurationStorage<EndPoint> ----

    IReadOnlySet<EndPoint> IClusterConfigurationStorage<EndPoint>.ActiveConfiguration
        => _innerGeneric.ActiveConfiguration;

    // Return all non-self peers as the proposed config.
    // DotNext unions this with the local node to form its full members list.
    IReadOnlySet<EndPoint>? IClusterConfigurationStorage<EndPoint>.ProposedConfiguration
        => _peers.Count > 0 ? _peers : null;

    ValueTask<bool> IClusterConfigurationStorage<EndPoint>.AddMemberAsync(EndPoint address, CancellationToken token)
        => _innerGeneric.AddMemberAsync(address, token);

    ValueTask<bool> IClusterConfigurationStorage<EndPoint>.RemoveMemberAsync(EndPoint address, CancellationToken token)
        => _innerGeneric.RemoveMemberAsync(address, token);

    event Func<EndPoint, bool, CancellationToken, ValueTask>? IClusterConfigurationStorage<EndPoint>.ActiveConfigurationChanged
    {
        add => _innerGeneric.ActiveConfigurationChanged += value;
        remove => _innerGeneric.ActiveConfigurationChanged -= value;
    }

    // ---- IClusterConfigurationStorage (non-generic, binary for Raft log replication) ----

    IClusterConfiguration IClusterConfigurationStorage.ActiveConfiguration
        => _innerNonGeneric.ActiveConfiguration;

    IClusterConfiguration? IClusterConfigurationStorage.ProposedConfiguration
        => _innerNonGeneric.ProposedConfiguration;

    ValueTask IClusterConfigurationStorage.LoadConfigurationAsync(CancellationToken token)
        => _innerNonGeneric.LoadConfigurationAsync(token);

    ValueTask IClusterConfigurationStorage.ProposeAsync(IClusterConfiguration configuration, CancellationToken token)
        => _innerNonGeneric.ProposeAsync(configuration, token);

    ValueTask IClusterConfigurationStorage.ApplyAsync(CancellationToken token)
        => _innerNonGeneric.ApplyAsync(token);

    Task IClusterConfigurationStorage.WaitForApplyAsync(CancellationToken token)
        => _innerNonGeneric.WaitForApplyAsync(token);

    public void Dispose() => _innerDisposable.Dispose();
}
