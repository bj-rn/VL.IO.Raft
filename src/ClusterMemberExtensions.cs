using DotNext.Net.Cluster;
using System.Net;

namespace VL.IO.Raft;

/// <summary>Extension methods for <see cref="IClusterMember"/>.</summary>
public static class ClusterMemberExtensions
{
    /// <summary>Splits a cluster member into its individual properties.</summary>
    /// <param name="member">The cluster member to split.</param>
    /// <param name="endPoint">Network address of this member.</param>
    /// <param name="isLeader">True if this member is the current leader.</param>
    /// <param name="isRemote">True if this is a remote member; false if it is the local node.</param>
    /// <param name="status">Availability status of this member (Unknown, Available, or Unavailable).</param>
    public static void Split(
        IClusterMember? member,
        out EndPoint? endPoint,
        out bool isLeader,
        out bool isRemote,
        out ClusterMemberStatus status)
    {
        endPoint = member?.EndPoint;
        isLeader = member?.IsLeader ?? false;
        isRemote = member?.IsRemote ?? false;
        status = member?.Status ?? ClusterMemberStatus.Unknown;
    }
}
