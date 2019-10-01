using EventStore.Core.Authentication;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.Util;
using Xunit;
using System.Net;
using System.Collections.Generic;

namespace EventStore.Core.Tests.Common.VNodeBuilderTests.when_building {
	public class with_default_settings_as_single_node : SingleNodeScenario {
		public override void Given() {
		}

		[Fact]
		public void should_create_single_cluster_node() {
			Assert.NotNull(_node);
			Assert.Equal(1, _settings.ClusterNodeCount);
			Assert.IsType<InternalAuthenticationProviderFactory>(_settings.AuthenticationProviderFactory);
			Assert.Equal(StatsStorage.Stream, _settings.StatsStorage);
		}

		[Fact]
		public void should_have_default_endpoints() {
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 1112), _settings.NodeInfo.InternalTcp);
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 1113), _settings.NodeInfo.ExternalTcp);
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 2112), _settings.NodeInfo.InternalHttp);
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 2113), _settings.NodeInfo.ExternalHttp);

			var intHttpPrefixes = new List<string> {"http://127.0.0.1:2112/"};
			var extHttpPrefixes = new List<string> {"http://127.0.0.1:2113/"};
			Assert.Equal(intHttpPrefixes, _settings.IntHttpPrefixes);
			Assert.Equal(extHttpPrefixes, _settings.ExtHttpPrefixes);
		}

		[Fact]
		public void should_not_use_ssl() {
			Assert.Equal("n/a", _settings.Certificate == null ? "n/a" : _settings.Certificate.ToString());
			Assert.False(_settings.UseSsl);
			Assert.Equal("n/a", _settings.SslTargetHost == null ? "n/a" : _settings.SslTargetHost);
		}

		[Fact]
		public void should_set_command_line_args_to_default_values() {
			Assert.Equal(Opts.EnableTrustedAuthDefault, _settings.EnableTrustedAuth);
			Assert.Equal(Opts.LogHttpRequestsDefault, _settings.LogHttpRequests);
			Assert.Equal(Opts.WorkerThreadsDefault, _settings.WorkerThreads);
			Assert.Equal(Opts.DiscoverViaDnsDefault, _settings.DiscoverViaDns);
			Assert.Equal(Opts.StatsPeriodDefault, _settings.StatsPeriod.Seconds);
			Assert.Equal(Opts.HistogramEnabledDefault, _settings.EnableHistograms);
			Assert.Equal(Opts.DisableHttpCachingDefault, _settings.DisableHTTPCaching);
			Assert.Equal(Opts.SkipDbVerifyDefault, !_settings.VerifyDbHash);
			Assert.Equal(Opts.MinFlushDelayMsDefault, _settings.MinFlushDelay.TotalMilliseconds);
			Assert.Equal(Opts.ScavengeHistoryMaxAgeDefault, _settings.ScavengeHistoryMaxAge);
			Assert.Equal(Opts.DisableScavengeMergeDefault, _settings.DisableScavengeMerging);
			Assert.Equal(Opts.AdminOnExtDefault, _settings.AdminOnPublic);
			Assert.Equal(Opts.StatsOnExtDefault, _settings.StatsOnPublic);
			Assert.Equal(Opts.GossipOnExtDefault, _settings.GossipOnPublic);
			Assert.Equal(Opts.MaxMemtableSizeDefault, _settings.MaxMemtableEntryCount);
			Assert.Equal(Opts.StartStandardProjectionsDefault, _settings.StartStandardProjections);
			Assert.Equal(Opts.UnsafeIgnoreHardDeleteDefault, _settings.UnsafeIgnoreHardDeletes);
			Assert.Equal(Opts.BetterOrderingDefault, _settings.BetterOrdering);
			Assert.True(string.IsNullOrEmpty(_settings.Index), "IndexPath");
			Assert.Equal(1, _settings.PrepareAckCount);
			Assert.Equal(1, _settings.CommitAckCount);
			Assert.Equal(Opts.PrepareTimeoutMsDefault, _settings.PrepareTimeout.TotalMilliseconds);
			Assert.Equal(Opts.CommitTimeoutMsDefault, _settings.CommitTimeout.TotalMilliseconds);

			Assert.Equal(Opts.IntTcpHeartbeatIntervalDefault, _settings.IntTcpHeartbeatInterval.TotalMilliseconds);
			Assert.Equal(Opts.IntTcpHeartbeatTimeoutDefault, _settings.IntTcpHeartbeatTimeout.TotalMilliseconds);
			Assert.Equal(Opts.ExtTcpHeartbeatIntervalDefault, _settings.ExtTcpHeartbeatInterval.TotalMilliseconds);
			Assert.Equal(Opts.ExtTcpHeartbeatTimeoutDefault, _settings.ExtTcpHeartbeatTimeout.TotalMilliseconds);

			Assert.Equal(TFConsts.ChunkSize, _dbConfig.ChunkSize);
			Assert.Equal(Opts.ChunksCacheSizeDefault, _dbConfig.MaxChunksCacheSize);
		}
	}

	public class with_default_settings_as_node_in_a_cluster : ClusterMemberScenario {
		public override void Given() {
		}

		[Fact]
		public void should_create_single_cluster_node() {
			Assert.NotNull(_node);
			Assert.Equal(_clusterSize, _settings.ClusterNodeCount);
			Assert.IsType<InternalAuthenticationProviderFactory>(_settings.AuthenticationProviderFactory);
			Assert.Equal(StatsStorage.Stream, _settings.StatsStorage);
		}

		[Fact]
		public void should_have_default_endpoints() {
			var internalTcp = new IPEndPoint(IPAddress.Loopback, 1112);
			var externalTcp = new IPEndPoint(IPAddress.Loopback, 1113);
			var internalHttp = new IPEndPoint(IPAddress.Loopback, 2112);
			var externalHttp = new IPEndPoint(IPAddress.Loopback, 2113);

			Assert.Equal(internalTcp, _settings.NodeInfo.InternalTcp);
			Assert.Equal(externalTcp, _settings.NodeInfo.ExternalTcp);
			Assert.Equal(internalHttp, _settings.NodeInfo.InternalHttp);
			Assert.Equal(externalHttp, _settings.NodeInfo.ExternalHttp);

			var intHttpPrefixes = new List<string> {"http://127.0.0.1:2112/"};
			var extHttpPrefixes = new List<string> {"http://127.0.0.1:2113/"};

			Assert.Equal(intHttpPrefixes, _settings.IntHttpPrefixes);
			Assert.Equal(extHttpPrefixes, _settings.ExtHttpPrefixes);

			Assert.Equal(internalTcp, _settings.GossipAdvertiseInfo.InternalTcp);
			Assert.Equal(externalTcp, _settings.GossipAdvertiseInfo.ExternalTcp);
			Assert.Equal(internalHttp, _settings.GossipAdvertiseInfo.InternalHttp);
			Assert.Equal(externalHttp, _settings.GossipAdvertiseInfo.ExternalHttp);
		}

		[Fact]
		public void should_not_use_ssl() {
			Assert.Equal("n/a", _settings.Certificate == null ? "n/a" : _settings.Certificate.ToString());
			Assert.False(_settings.UseSsl);
			Assert.Equal("n/a", _settings.SslTargetHost == null ? "n/a" : _settings.SslTargetHost.ToString());
		}

		[Fact]
		public void should_set_command_line_args_to_default_values() {
			Assert.Equal(Opts.EnableTrustedAuthDefault, _settings.EnableTrustedAuth);
			Assert.Equal(Opts.LogHttpRequestsDefault, _settings.LogHttpRequests);
			Assert.Equal(Opts.WorkerThreadsDefault, _settings.WorkerThreads);
			Assert.Equal(Opts.DiscoverViaDnsDefault, _settings.DiscoverViaDns);
			Assert.Equal(Opts.StatsPeriodDefault, _settings.StatsPeriod.Seconds);
			Assert.Equal(Opts.HistogramEnabledDefault, _settings.EnableHistograms);
			Assert.Equal(Opts.DisableHttpCachingDefault, _settings.DisableHTTPCaching);
			Assert.Equal(Opts.SkipDbVerifyDefault, !_settings.VerifyDbHash);
			Assert.Equal(Opts.MinFlushDelayMsDefault, _settings.MinFlushDelay.TotalMilliseconds);
			Assert.Equal(Opts.ScavengeHistoryMaxAgeDefault, _settings.ScavengeHistoryMaxAge);
			Assert.Equal(Opts.DisableScavengeMergeDefault, _settings.DisableScavengeMerging);
			Assert.Equal(Opts.AdminOnExtDefault, _settings.AdminOnPublic);
			Assert.Equal(Opts.StatsOnExtDefault, _settings.StatsOnPublic);
			Assert.Equal(Opts.GossipOnExtDefault, _settings.GossipOnPublic);
			Assert.Equal(Opts.MaxMemtableSizeDefault, _settings.MaxMemtableEntryCount);
			Assert.Equal(Opts.StartStandardProjectionsDefault, _settings.StartStandardProjections);
			Assert.Equal(Opts.UnsafeIgnoreHardDeleteDefault, _settings.UnsafeIgnoreHardDeletes);
			Assert.Equal(Opts.BetterOrderingDefault, _settings.BetterOrdering);
			Assert.True(string.IsNullOrEmpty(_settings.Index), "IndexPath");
			Assert.Equal(Opts.PrepareTimeoutMsDefault, _settings.PrepareTimeout.TotalMilliseconds);
			Assert.Equal(Opts.CommitTimeoutMsDefault, _settings.CommitTimeout.TotalMilliseconds);

			Assert.Equal(Opts.IntTcpHeartbeatIntervalDefault, _settings.IntTcpHeartbeatInterval.TotalMilliseconds);
			Assert.Equal(Opts.IntTcpHeartbeatTimeoutDefault, _settings.IntTcpHeartbeatTimeout.TotalMilliseconds);
			Assert.Equal(Opts.ExtTcpHeartbeatIntervalDefault, _settings.ExtTcpHeartbeatInterval.TotalMilliseconds);
			Assert.Equal(Opts.ExtTcpHeartbeatTimeoutDefault, _settings.ExtTcpHeartbeatTimeout.TotalMilliseconds);

			Assert.Equal(TFConsts.ChunkSize, _dbConfig.ChunkSize);
			Assert.Equal(Opts.ChunksCacheSizeDefault, _dbConfig.MaxChunksCacheSize);
			Assert.Equal(Opts.AlwaysKeepScavengedDefault, _settings.AlwaysKeepScavenged);
		}

		[Fact]
		public void should_set_commit_and_prepare_counts_to_quorum_size() {
			var quorumSize = _clusterSize / 2 + 1;
			Assert.Equal(quorumSize, _settings.PrepareAckCount);
			Assert.Equal(quorumSize, _settings.CommitAckCount);
		}
	}
}
