using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Linq;
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
    private bool _lastLog = true;
    private volatile bool _log = true;
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
    /// <param name="log">When true (default), Raft internal messages are written to the vvvv log. Errors written to the Error pin are unaffected.</param>
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
        [Pin(Visibility = PinVisibility.Optional)] bool log = true,
        bool enable = false )
    {
        if (enable)
        {
            if (!ReferenceEquals(hosts, _lastHosts)
                || port != _lastPort
                || localId != _lastLocalId
                || prioritize != _lastPrioritize
                || log != _lastLog)
            {
                StopCluster();
                _lastHosts = hosts;
                _lastPort = port;
                _lastLocalId = localId;
                _lastPrioritize = prioritize;
                _lastLog = log;
                if (localId >= 0 && localId < hosts.Count)
                {
                    try { StartCluster(hosts, port, localId, prioritize, log); }
                    catch (Exception ex)
                    {
                        var inner = ex.GetBaseException();
                        _error = inner.Message;
                        _logger.LogError(inner, "Raft cluster failed to start");
                    }
                }
                else
                {
                    _error = $"LocalId {localId} is out of range for {hosts.Count} hosts.";
                }
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

    private void StartCluster(Spread<string> hosts, int port, int localId, bool prioritize, bool log)
    {
        _error = "";
        _log = log;
        var localEndpoint = MakeEndpoint(hosts[localId], port);
        var config = new RaftCluster.TcpConfiguration(localEndpoint)
        {
            LoggerFactory = log ? _loggerFactory : NullLoggerFactory.Instance,
            ColdStart = true,
        };

        if (prioritize)
        {
            // Lower ID → shorter timeout → becomes candidate first → wins election.
            // Node 0: 150–300 ms, node 1: 200–350 ms, etc.
            config.LowerElectionTimeout = 150 + localId * 50;
            config.UpperElectionTimeout = 300 + localId * 50;
        }

        var peers = Enumerable.Range(0, hosts.Count)
            .Where(i => i != localId)
            .Select(i => (EndPoint)MakeEndpoint(hosts[i], port));
        config.ConfigurationStorage = new StaticConfigStorage(peers);

        _cluster = new RaftCluster(config);
        _cluster.LeaderChanged += OnLeaderChanged;

        _ = _cluster.StartAsync(CancellationToken.None).ContinueWith(
            t =>
            {
                var ex = t.Exception!.GetBaseException();
                _error = ex.Message;
                if (_log) _logger.LogError(ex, "Raft cluster failed to start");
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
            if (_log) _logger.LogError(inner, "Raft cluster failed to stop");
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
        if (!_log) return;
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
