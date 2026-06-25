using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using System.Net;
using VL.Core.Import;
using VL.Lib.Collections;
using VL.Model;

namespace VL.IO.Raft;

/// <summary>
/// Runs a Raft consensus cluster among a fixed set of known machines and surfaces the elected leader.
/// All machines must use the same Hosts list and Port. Set LocalId to this machine's index in the list;
/// lower index means higher election priority (more likely to become leader).
/// </summary>
[ProcessNode]
public sealed class LeaderElection : IDisposable
{
    private RaftCluster? _cluster;
    private int _inputHash;
    private volatile string _leader = "";
    private volatile bool _isLeader;
    private volatile bool _hasLeader;
    private volatile string _error = "";

    /// <summary>
    /// Updates leader election state. Enable must be true for the cluster to start.
    /// </summary>
    /// <param name="leader">Host:port of the current leader, empty if no leader is known yet.</param>
    /// <param name="isLeader">True if this machine is the current leader.</param>
    /// <param name="hasLeader">True if any leader is known.</param>
    /// <param name="error">Error message if the cluster failed to start, otherwise empty.</param>
    /// <param name="hosts">IP addresses or hostnames of all machines in the cluster, in the same order on every machine.</param>
    /// <param name="port">TCP port all machines listen on.</param>
    /// <param name="localId">Index of this machine in the Hosts list. Lower index means higher election priority (more likely to become leader).</param>
    /// <param name="enable">Set to true to start the cluster.</param>
    public void Update(
        out string leader,
        out bool isLeader,
        out bool hasLeader,
        [Pin(Visibility = PinVisibility.Optional)] out string error,
        Spread<string> hosts,
        int port = 3262,
        int localId = 0,
        bool enable = false)
    {
        var hash = ComputeHash(hosts, port, localId);

        if (enable)
        {
            if (hash != _inputHash)
            {
                StopCluster();
                _inputHash = hash;
                if (localId >= 0 && localId < hosts.Count)
                    StartCluster(hosts, port, localId);
            }
        }
        else if (_cluster != null)
        {
            StopCluster();
            _inputHash = 0;
        }

        leader = _leader;
        isLeader = _isLeader;
        hasLeader = _hasLeader;
        error = _error;
    }

    private void StartCluster(Spread<string> hosts, int port, int localId)
    {
        _error = "";
        var localEndpoint = MakeEndpoint(hosts[localId], port);
        var config = new RaftCluster.TcpConfiguration(localEndpoint)
        {
            ColdStart = true,
            // Lower ID → shorter timeout → becomes candidate first → wins election.
            // Node 0: 150–300 ms, node 1: 200–350 ms, etc.
            LowerElectionTimeout = 150 + localId * 50,
            UpperElectionTimeout = 300 + localId * 50,
        };

        var storage = config.UseInMemoryConfigurationStorage();
        for (int i = 0; i < hosts.Count; i++)
            storage.AddMember(MakeEndpoint(hosts[i], port));

        _cluster = new RaftCluster(config);
        _cluster.LeaderChanged += OnLeaderChanged;

        _ = _cluster.StartAsync(CancellationToken.None).ContinueWith(
            t => _error = t.Exception?.GetBaseException().Message ?? "Start failed",
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }

    private void StopCluster()
    {
        if (_cluster is null) return;
        _cluster.LeaderChanged -= OnLeaderChanged;
        try { _cluster.StopAsync(CancellationToken.None).Wait(3000); }
        catch { }
        _cluster.Dispose();
        _cluster = null;
        _leader = "";
        _isLeader = false;
        _hasLeader = false;
    }

    private void OnLeaderChanged(ICluster cluster, IClusterMember? leader)
    {
        _hasLeader = leader != null;
        _leader = leader?.EndPoint?.ToString() ?? "";
        _isLeader = leader != null && !leader.IsRemote;
    }

    public void Dispose() => StopCluster();

    private static int ComputeHash(Spread<string> hosts, int port, int localId)
    {
        var hc = new HashCode();
        hc.Add(port);
        hc.Add(localId);
        for (int i = 0; i < hosts.Count; i++)
            hc.Add(hosts[i]);
        return hc.ToHashCode();
    }

    private static IPEndPoint MakeEndpoint(string host, int port)
    {
        var ip = IPAddress.TryParse(host, out var addr)
            ? addr
            : Dns.GetHostAddresses(host)[0];
        return new IPEndPoint(ip, port);
    }
}
