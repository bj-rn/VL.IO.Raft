using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using Microsoft.Extensions.Logging;
using System.Net;
using VL.Core;
using VL.Core.Import;
using VL.Lib.Collections;
using VL.Model;

namespace VL.IO.Raft;

/// <summary>
/// Runs a Raft consensus cluster among a fixed set of known machines and surfaces the elected leader.
/// All machines must use the same Hosts list and Port. Set LocalId to this machine's index in the list;
/// When Prioritize is enabled, lower index means higher election priority (more likely to become leader).
/// </summary>
[ProcessNode]
public sealed class LeaderElection : IDisposable
{
    private readonly ILogger _logger;
    private readonly ILoggerFactory _loggerFactory;
    private RaftCluster? _cluster;
    private Spread<string>? _lastHosts;
    private int _lastPort;
    private int _lastLocalId;
    private bool _lastPrioritize;
    private volatile string _leader = "";
    private volatile bool _isLeader;
    private volatile bool _hasLeader;
    private volatile string _error = "";

    /// <inheritdoc />
    public LeaderElection(NodeContext nodeContext)
    {
        _loggerFactory = nodeContext.AppHost.LoggerFactory;
        _logger = _loggerFactory.CreateLogger<LeaderElection>();
    }

    /// <summary>
    /// Updates leader election state. Enable must be true for the cluster to start.
    /// </summary>
    /// <param name="leader">Host:port of the current leader, empty if no leader is known yet.</param>
    /// <param name="isLeader">True if this machine is the current leader.</param>
    /// <param name="hasLeader">True if any leader is known.</param>
    /// <param name="error">Error message if the cluster failed to start, otherwise empty.</param>
    /// <param name="hosts">IP addresses or hostnames of all machines in the cluster, in the same order on every machine.</param>
    /// <param name="port">TCP port all machines listen on.</param>
    /// <param name="localId">Index of this machine in the Hosts list. When Prioritize is enabled, lower index means higher election priority (more likely to become leader).</param>
    /// <param name="prioritize">When true, scales election timeouts by LocalId so the node with the lowest index is most likely to win. When false, all nodes have equal chance (standard Raft behaviour).</param>
    /// <param name="enable">Set to true to start the cluster.</param>
    public void Update(
        out string leader,
        out bool isLeader,
        out bool hasLeader,
        [Pin(Visibility = PinVisibility.Optional)] out string error,
        Spread<string> hosts,
        int port = 3262,
        int localId = 0,
        bool prioritize = false,
        bool enable = false)
    {
        if (enable)
        {
            if (!ReferenceEquals(hosts, _lastHosts)
                || port != _lastPort
                || localId != _lastLocalId
                || prioritize != _lastPrioritize)
            {
                StopCluster();
                _lastHosts = hosts;
                _lastPort = port;
                _lastLocalId = localId;
                _lastPrioritize = prioritize;
                if (localId >= 0 && localId < hosts.Count)
                    StartCluster(hosts, port, localId, prioritize);
            }
        }
        else if (_cluster != null)
        {
            StopCluster();
            _lastHosts = null;
        }

        leader = _leader;
        isLeader = _isLeader;
        hasLeader = _hasLeader;
        error = _error;
    }

    private void StartCluster(Spread<string> hosts, int port, int localId, bool prioritize)
    {
        _error = "";
        var localEndpoint = MakeEndpoint(hosts[localId], port);
        var config = new RaftCluster.TcpConfiguration(localEndpoint)
        {
            LoggerFactory = _loggerFactory,
            // All nodes bootstrap simultaneously with the same full member list.
            // ColdStart = true means "start a new cluster / hold an initial election",
            // as opposed to false which means "join an already-running cluster".
            ColdStart = true,
        };

        if (prioritize)
        {
            // Lower ID → shorter timeout → becomes candidate first → wins election.
            // Node 0: 150–300 ms, node 1: 200–350 ms, etc.
            config.LowerElectionTimeout = 150 + localId * 50;
            config.UpperElectionTimeout = 300 + localId * 50;
        }

        // Initialise the ACTIVE configuration with all members (including self).
        // AddMember() only proposes a change and silently rejects subsequent calls — Build() must be used
        // to set the active membership before the cluster starts so all nodes have the same view.
        var storage = config.UseInMemoryConfigurationStorage();
        var memberBuilder = storage.CreateActiveConfigurationBuilder();
        for (int i = 0; i < hosts.Count; i++)
            memberBuilder.Add(MakeEndpoint(hosts[i], port));
        memberBuilder.Build();

        _cluster = new RaftCluster(config);
        _cluster.LeaderChanged += OnLeaderChanged;

        _ = _cluster.StartAsync(CancellationToken.None).ContinueWith(
            t =>
            {
                var ex = t.Exception?.GetBaseException();
                _error = ex?.Message ?? "Start failed";
                _logger.LogError(ex, "Raft cluster failed to start");
            },
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }

    private void StopCluster()
    {
        if (_cluster is null) return;
        _cluster.LeaderChanged -= OnLeaderChanged;
        try { _cluster.StopAsync(CancellationToken.None).Wait(3000); }
        catch (Exception ex)
        {
            var inner = ex.GetBaseException();
            _error = inner.Message;
            _logger.LogError(inner, "Raft cluster failed to stop");
        }
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
        if (leader is null)
            _logger.LogInformation("Raft: leader lost");
        else
            _logger.LogInformation("Raft: new leader {Leader} (isLocal={IsLocal})", _leader, _isLeader);
    }


    /// <inheritdoc />
    public void Dispose() => StopCluster();

    private static IPEndPoint MakeEndpoint(string host, int port)
    {
        var ip = IPAddress.TryParse(host, out var addr)
            ? addr
            : Dns.GetHostAddresses(host)[0];
        return new IPEndPoint(ip, port);
    }
}
