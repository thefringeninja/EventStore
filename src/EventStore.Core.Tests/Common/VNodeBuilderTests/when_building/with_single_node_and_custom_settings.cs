using Xunit;
using System;
using System.IO;
using System.Net;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.Services.Monitoring;
using System.Collections.Generic;
using EventStore.Common.Utils;

namespace EventStore.Core.Tests.Common.VNodeBuilderTests.when_building {
	public class with_run_on_disk : SingleNodeScenario {
		private string _dbPath;

		public override void Given() {
			_dbPath = Path.Combine(Path.GetTempPath(), string.Format("Test-{0}", Guid.NewGuid()));
			_builder.RunOnDisk(_dbPath);
		}

		[Fact]
		public void should_set_memdb_to_false() {
			Assert.False(_dbConfig.InMemDb);
		}

		[Fact]
		public void should_set_the_db_path() {
			Assert.Equal(_dbPath, _dbConfig.Path);
		}
	}

	public class with_default_endpoints_option : SingleNodeScenario {
		public override void Given() {
			var noEndpoint = new IPEndPoint(IPAddress.None, 0);
			_builder.WithInternalHttpOn(noEndpoint)
				.WithExternalHttpOn(noEndpoint)
				.WithInternalTcpOn(noEndpoint)
				.WithExternalTcpOn(noEndpoint);

			_builder.OnDefaultEndpoints();
		}

		[Fact]
		public void should_set_internal_tcp() {
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 1112), _settings.NodeInfo.InternalTcp);
		}

		[Fact]
		public void should_set_external_tcp() {
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 1113), _settings.NodeInfo.ExternalTcp);
		}

		[Fact]
		public void should_set_internal_http() {
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 2112), _settings.NodeInfo.InternalHttp);
		}

		[Fact]
		public void should_set_external_http() {
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 2113), _settings.NodeInfo.ExternalHttp);
		}
	}

	public class with_run_in_memory : SingleNodeScenario {
		public override void Given() {
			_builder.RunInMemory();
		}

		[Fact]
		public void should_set_memdb_to_true() {
			Assert.True(_dbConfig.InMemDb);
		}
	}

	public class with_log_http_requests_enabled : SingleNodeScenario {
		public override void Given() {
			_builder.EnableLoggingOfHttpRequests();
		}

		[Fact]
		public void should_set_http_logging_to_true() {
			Assert.True(_settings.LogHttpRequests);
		}
	}

	public class with_custom_number_of_worker_threads : SingleNodeScenario {
		public override void Given() {
			_builder.WithWorkerThreads(10);
		}

		[Fact]
		public void should_set_the_number_of_worker_threads() {
			Assert.Equal(10, _settings.WorkerThreads);
		}
	}

	public class with_custom_stats_period : SingleNodeScenario {
		public override void Given() {
			_builder.WithStatsPeriod(TimeSpan.FromSeconds(1));
		}

		[Fact]
		public void should_set_the_stats_period() {
			Assert.Equal(1, _settings.StatsPeriod.TotalSeconds);
		}
	}

	public class with_custom_stats_storage : SingleNodeScenario {
		public override void Given() {
			_builder.WithStatsStorage(StatsStorage.None);
		}

		[Fact]
		public void should_set_the_stats_storage() {
			Assert.Equal(StatsStorage.None, _settings.StatsStorage);
		}
	}

	public class with_histograms_enabled : SingleNodeScenario {
		public override void Given() {
			_builder.EnableHistograms();
		}

		[Fact]
		public void should_enable_histograms() {
			Assert.True(_settings.EnableHistograms);
		}
	}

	public class with_trusted_auth_enabled : SingleNodeScenario {
		public override void Given() {
			_builder.EnableTrustedAuth();
		}

		[Fact]
		public void should_enable_trusted_authentication() {
			Assert.True(_settings.EnableTrustedAuth);
		}
	}

	public class with_http_caching_disabled : SingleNodeScenario {
		public override void Given() {
			_builder.DisableHTTPCaching();
		}

		[Fact]
		public void should_disable_http_caching() {
			Assert.True(_settings.DisableHTTPCaching);
		}
	}

	public class without_verifying_db_hashes : SingleNodeScenario {
		public override void Given() {
			_builder.DoNotVerifyDbHashes();
		}

		[Fact]
		public void should_set_verify_db_hashes_to_false() {
			Assert.False(_settings.VerifyDbHash);
		}
	}

	public class with_verifying_db_hashes : SingleNodeScenario {
		public override void Given() {
			_builder.DoNotVerifyDbHashes() // Turn off verification before turning it back on
				.VerifyDbHashes();
		}

		[Fact]
		public void should_set_verify_db_hashes_to_true() {
			Assert.True(_settings.VerifyDbHash);
		}
	}

	public class with_custom_min_flush_delay : SingleNodeScenario {
		public override void Given() {
			_builder.WithMinFlushDelay(TimeSpan.FromMilliseconds(1200));
		}

		[Fact]
		public void should_set_the_min_flush_delay() {
			Assert.Equal(1200, _settings.MinFlushDelay.TotalMilliseconds);
		}
	}

	public class with_custom_scavenge_history_max_age : SingleNodeScenario {
		public override void Given() {
			_builder.WithScavengeHistoryMaxAge(2);
		}

		[Fact]
		public void should_set_the_scavenge_history_max_age() {
			Assert.Equal(2, _settings.ScavengeHistoryMaxAge);
		}
	}

	public class with_scavenge_merging_disabled : SingleNodeScenario {
		public override void Given() {
			_builder.DisableScavengeMerging();
		}

		[Fact]
		public void should_disable_scavenge_merging() {
			Assert.True(_settings.DisableScavengeMerging);
		}
	}

	public class with_custom_max_memtable_size : SingleNodeScenario {
		public override void Given() {
			_builder.MaximumMemoryTableSizeOf(200);
		}

		[Fact]
		public void should_set_the_max_memtable_size() {
			Assert.Equal(200, _settings.MaxMemtableEntryCount);
		}
	}

	public class with_custom_hash_collision_read_limit : SingleNodeScenario {
		public override void Given() {
			_builder.WithHashCollisionReadLimitOf(200);
		}

		[Fact]
		public void should_set_the_hash_collision_read_limit() {
			Assert.Equal(200, _settings.HashCollisionReadLimit);
		}
	}

	public class with_standard_projections_started : SingleNodeScenario {
		public override void Given() {
			_builder.StartStandardProjections();
		}

		[Fact]
		public void should_set_start_standard_projections_to_true() {
			Assert.True(_settings.StartStandardProjections);
		}
	}

	public class with_ignore_hard_delete_enabled : SingleNodeScenario {
		public override void Given() {
			_builder.WithUnsafeIgnoreHardDelete();
		}

		[Fact]
		public void should_set_ignore_hard_deletes() {
			Assert.True(_settings.UnsafeIgnoreHardDeletes);
		}
	}

	public class with_better_ordering_enabled : SingleNodeScenario {
		public override void Given() {
			_builder.WithBetterOrdering();
		}

		[Fact]
		public void should_set_better_ordering() {
			Assert.True(_settings.BetterOrdering);
		}
	}

	public class with_custom_index_path : SingleNodeScenario {
		public override void Given() {
			_builder.WithIndexPath("index");
		}

		[Fact]
		public void should_set_the_index_path() {
			Assert.Equal("index", _settings.Index);
		}
	}

	public class with_custom_prepare_timeout : SingleNodeScenario {
		public override void Given() {
			_builder.WithPrepareTimeout(TimeSpan.FromMilliseconds(1234));
		}

		[Fact]
		public void should_set_the_prepare_timeout() {
			Assert.Equal(1234, _settings.PrepareTimeout.TotalMilliseconds);
		}
	}

	public class with_custom_commit_timeout : SingleNodeScenario {
		public override void Given() {
			_builder.WithCommitTimeout(TimeSpan.FromMilliseconds(1234));
		}

		[Fact]
		public void should_set_the_commit_timeout() {
			Assert.Equal(1234, _settings.CommitTimeout.TotalMilliseconds);
		}
	}

	public class with_custom_internal_heartbeat_interval : SingleNodeScenario {
		public override void Given() {
			_builder.WithInternalHeartbeatInterval(TimeSpan.FromMilliseconds(1234));
		}

		[Fact]
		public void should_set_the_internal_heartbeat_interval() {
			Assert.Equal(1234, _settings.IntTcpHeartbeatInterval.TotalMilliseconds);
		}
	}

	public class with_custom_internal_heartbeat_timeout : SingleNodeScenario {
		public override void Given() {
			_builder.WithInternalHeartbeatTimeout(TimeSpan.FromMilliseconds(1234));
		}

		[Fact]
		public void should_set_the_internal_heartbeat_timeout() {
			Assert.Equal(1234, _settings.IntTcpHeartbeatTimeout.TotalMilliseconds);
		}
	}

	public class with_custom_external_heartbeat_interval : SingleNodeScenario {
		public override void Given() {
			_builder.WithExternalHeartbeatInterval(TimeSpan.FromMilliseconds(1234));
		}

		[Fact]
		public void should_set_the_external_heartbeat_interval() {
			Assert.Equal(1234, _settings.ExtTcpHeartbeatInterval.TotalMilliseconds);
		}
	}

	public class with_custom_external_heartbeat_timeout : SingleNodeScenario {
		public override void Given() {
			_builder.WithExternalHeartbeatTimeout(TimeSpan.FromMilliseconds(1234));
		}

		[Fact]
		public void should_set_the_external_heartbeat_timeout() {
			Assert.Equal(1234, _settings.ExtTcpHeartbeatTimeout.TotalMilliseconds);
		}
	}

	public class with_no_admin_on_public_interface : SingleNodeScenario {
		public override void Given() {
			_builder.NoAdminOnPublicInterface();
		}

		[Fact]
		public void should_disable_admin_on_public() {
			Assert.False(_settings.AdminOnPublic);
		}
	}

	public class with_no_gossip_on_public_interface : SingleNodeScenario {
		public override void Given() {
			_builder.NoGossipOnPublicInterface();
		}

		[Fact]
		public void should_disable_gossip_on_public() {
			Assert.False(_settings.GossipOnPublic);
		}
	}

	public class with_no_stats_on_public_interface : SingleNodeScenario {
		public override void Given() {
			_builder.NoStatsOnPublicInterface();
		}

		[Fact]
		public void should_disable_stats_on_public() {
			Assert.False(_settings.StatsOnPublic);
		}
	}

	public class with_custom_ip_endpoints : SingleNodeScenario {
		private IPEndPoint _internalHttp;
		private IPEndPoint _externalHttp;
		private IPEndPoint _internalTcp;
		private IPEndPoint _externalTcp;

		public override void Given() {
			var baseIpAddress = IPAddress.Parse("127.0.1.15");
			_internalHttp = new IPEndPoint(baseIpAddress, 1112);
			_externalHttp = new IPEndPoint(baseIpAddress, 1113);
			_internalTcp = new IPEndPoint(baseIpAddress, 1114);
			_externalTcp = new IPEndPoint(baseIpAddress, 1115);
			_builder.WithInternalHttpOn(_internalHttp)
				.WithExternalHttpOn(_externalHttp)
				.WithExternalTcpOn(_externalTcp)
				.WithInternalTcpOn(_internalTcp);
		}

		[Fact]
		public void should_set_internal_http_endpoint() {
			Assert.Equal(_internalHttp, _settings.NodeInfo.InternalHttp);
		}

		[Fact]
		public void should_set_external_http_endpoint() {
			Assert.Equal(_externalHttp, _settings.NodeInfo.ExternalHttp);
		}

		[Fact]
		public void should_set_internal_tcp_endpoint() {
			Assert.Equal(_internalTcp, _settings.NodeInfo.InternalTcp);
		}

		[Fact]
		public void should_set_external_tcp_endpoint() {
			Assert.Equal(_externalTcp, _settings.NodeInfo.ExternalTcp);
		}

		[Fact]
		public void should_set_internal_http_prefixes() {
			var internalHttpPrefix = string.Format("http://{0}/", _internalHttp);
			Assert.Equal(new string[] {internalHttpPrefix}, _settings.IntHttpPrefixes);
		}

		[Fact]
		public void should_set_external_http_prefixes() {
			var externalHttpPrefix = string.Format("http://{0}/", _externalHttp);
			Assert.Equal(new string[] {externalHttpPrefix}, _settings.ExtHttpPrefixes);
		}
	}

	public class with_custom_http_prefixes : SingleNodeScenario {
		private string _intPrefix;
		private string _intLoopbackPrefix;
		private string _extPrefix;
		private string _extLoopbackPrefix;

		public override void Given() {
			var baseIpAddress = IPAddress.Parse("127.0.1.15");
			int intPort = 1112;
			int extPort = 1113;

			var internalHttp = new IPEndPoint(baseIpAddress, intPort);
			var externalHttp = new IPEndPoint(baseIpAddress, extPort);

			_intPrefix = string.Format("http://{0}/", internalHttp);
			_intLoopbackPrefix = string.Format("http://{0}/", new IPEndPoint(IPAddress.Loopback, intPort));
			_extPrefix = string.Format("http://{0}/", externalHttp);
			_extLoopbackPrefix = string.Format("http://{0}/", new IPEndPoint(IPAddress.Loopback, extPort));

			_builder.WithInternalHttpOn(internalHttp)
				.WithExternalHttpOn(externalHttp)
				.AddInternalHttpPrefix(_intPrefix)
				.AddInternalHttpPrefix(_intLoopbackPrefix)
				.AddExternalHttpPrefix(_extPrefix)
				.AddExternalHttpPrefix(_extLoopbackPrefix);
		}

		[Fact]
		public void should_set_internal_http_prefixes() {
			Assert.Equal(new[] {_intPrefix, _intLoopbackPrefix}, _settings.IntHttpPrefixes);
		}

		[Fact]
		public void should_set_external_http_prefixes() {
			Assert.Equal(new[] {_extPrefix, _extLoopbackPrefix}, _settings.ExtHttpPrefixes);
		}
	}

	public class with_add_interface_prefixes : SingleNodeScenario {
		private IPEndPoint _internalHttp;
		private IPEndPoint _externalHttp;
		private IPEndPoint _internalTcp;
		private IPEndPoint _externalTcp;

		public override void Given() {
			var baseIpAddress = IPAddress.Loopback;
			_internalHttp = new IPEndPoint(baseIpAddress, 1112);
			_externalHttp = new IPEndPoint(baseIpAddress, 1113);
			_internalTcp = new IPEndPoint(baseIpAddress, 1114);
			_externalTcp = new IPEndPoint(baseIpAddress, 1115);
			_builder.WithInternalHttpOn(_internalHttp)
				.WithExternalHttpOn(_externalHttp)
				.WithExternalTcpOn(_externalTcp)
				.WithInternalTcpOn(_internalTcp);
		}

		[Fact]
		public void should_set_internal_http_prefixes() {
			var internalHttpPrefixes = new List<string> {
				string.Format("http://{0}/", _internalHttp)
			};
			Assert.Equal(internalHttpPrefixes, _settings.IntHttpPrefixes);
		}

		[Fact]
		public void should_set_external_http_prefixes() {
			var externalHttpPrefixes = new List<string> {
				string.Format("http://{0}/", _externalHttp)
			};
			Assert.Equal(externalHttpPrefixes, _settings.ExtHttpPrefixes);
		}
	}

	public class with_dont_add_interface_prefixes : SingleNodeScenario {
		private IPEndPoint _internalHttp;
		private IPEndPoint _externalHttp;
		private IPEndPoint _internalTcp;
		private IPEndPoint _externalTcp;

		public override void Given() {
			var baseIpAddress = IPAddress.Loopback;
			_internalHttp = new IPEndPoint(baseIpAddress, 1112);
			_externalHttp = new IPEndPoint(baseIpAddress, 1113);
			_internalTcp = new IPEndPoint(baseIpAddress, 1114);
			_externalTcp = new IPEndPoint(baseIpAddress, 1115);
			_builder.WithInternalHttpOn(_internalHttp)
				.WithExternalHttpOn(_externalHttp)
				.WithExternalTcpOn(_externalTcp)
				.WithInternalTcpOn(_internalTcp)
				.DontAddInterfacePrefixes();
		}

		[Fact]
		public void should_set_no_internal_http_prefixes() {
			Assert.Empty(_settings.IntHttpPrefixes);
		}

		[Fact]
		public void should_set_no_external_http_prefixes() {
			Assert.Empty(_settings.ExtHttpPrefixes);
		}
	}

	public class with_custom_index_cache_depth : SingleNodeScenario {
		public override void Given() {
			_builder.WithIndexCacheDepth(8);
		}

		[Fact]
		public void should_set_index_cache_depth() {
			Assert.Equal(8, _settings.IndexCacheDepth);
		}
	}

	public class with_custom_authentication_provider_factory : SingleNodeScenario {
		public override void Given() {
			_builder.WithAuthenticationProvider(new TestAuthenticationProviderFactory());
		}

		[Fact]
		public void should_set_authentication_provider_factory() {
			Assert.IsType<TestAuthenticationProviderFactory>(_settings.AuthenticationProviderFactory);
		}
	}

	public class with_custom_chunk_size : SingleNodeScenario {
		private int _chunkSize;

		public override void Given() {
			_chunkSize = 268435712;
			_builder.WithTfChunkSize(_chunkSize);
		}

		[Fact]
		public void should_set_chunk_size() {
			Assert.Equal(_chunkSize, _dbConfig.ChunkSize);
		}
	}

	public class with_custom_chunk_cache_size : SingleNodeScenario {
		private long _chunkCacheSize;

		public override void Given() {
			_chunkCacheSize = 268435712;
			_builder.WithTfChunksCacheSize(_chunkCacheSize);
		}

		[Fact]
		public void should_set_max_chunk_cache_size() {
			Assert.Equal(_chunkCacheSize, _dbConfig.MaxChunksCacheSize);
		}
	}

	public class with_custom_number_of_cached_chunks : SingleNodeScenario {
		public override void Given() {
			_builder.WithTfCachedChunks(10);
		}

		[Fact]
		public void should_set_max_chunk_size_to_the_size_of_the_number_of_cached_chunks() {
			var chunkSizeResult = 10 * (long)(TFConsts.ChunkSize + ChunkHeader.Size + ChunkFooter.Size);
			Assert.Equal(chunkSizeResult, _dbConfig.MaxChunksCacheSize);
		}
	}

	public class with_custom_advertise_as : SingleNodeScenario {
		private Data.GossipAdvertiseInfo _advertiseInfo;

		public override void Given() {
			var internalIPToAdvertise = IPAddress.Parse("127.0.1.1");
			var externalIPToAdvertise = IPAddress.Parse("127.0.1.2");
			var intTcpEndpoint = new IPEndPoint(internalIPToAdvertise, 1111);
			var intSecTcpEndpoint = new IPEndPoint(internalIPToAdvertise, 1112);
			var extTcpEndpoint = new IPEndPoint(externalIPToAdvertise, 1113);
			var extSecTcpEndpoint = new IPEndPoint(externalIPToAdvertise, 1114);
			var intHttpEndpoint = new IPEndPoint(internalIPToAdvertise, 1115);
			var extHttpEndpoint = new IPEndPoint(externalIPToAdvertise, 1116);

			_advertiseInfo = new Data.GossipAdvertiseInfo(intTcpEndpoint, intSecTcpEndpoint, extTcpEndpoint,
				extSecTcpEndpoint, intHttpEndpoint, extHttpEndpoint, internalIPToAdvertise, externalIPToAdvertise,
				intHttpEndpoint.Port, extHttpEndpoint.Port);

			_builder.AdvertiseInternalIPAs(internalIPToAdvertise)
				.AdvertiseExternalIPAs(externalIPToAdvertise)
				.AdvertiseInternalTCPPortAs(intTcpEndpoint.Port)
				.AdvertiseExternalTCPPortAs(extTcpEndpoint.Port)
				.AdvertiseInternalSecureTCPPortAs(intSecTcpEndpoint.Port)
				.AdvertiseExternalSecureTCPPortAs(extSecTcpEndpoint.Port)
				.AdvertiseInternalHttpPortAs(intHttpEndpoint.Port)
				.AdvertiseExternalHttpPortAs(extHttpEndpoint.Port);
		}

		[Fact]
		public void should_set_the_advertise_as_info_to_the_specified() {
			Assert.Equal(_advertiseInfo.InternalTcp, _settings.GossipAdvertiseInfo.InternalTcp);
			Assert.Equal(_advertiseInfo.ExternalTcp, _settings.GossipAdvertiseInfo.ExternalTcp);
			Assert.Equal(_advertiseInfo.InternalSecureTcp, _settings.GossipAdvertiseInfo.InternalSecureTcp);
			Assert.Equal(_advertiseInfo.ExternalSecureTcp, _settings.GossipAdvertiseInfo.ExternalSecureTcp);
			Assert.Equal(_advertiseInfo.InternalHttp, _settings.GossipAdvertiseInfo.InternalHttp);
			Assert.Equal(_advertiseInfo.ExternalHttp, _settings.GossipAdvertiseInfo.ExternalHttp);
			Assert.Equal(_advertiseInfo.AdvertiseInternalIPAs, _settings.GossipAdvertiseInfo.AdvertiseInternalIPAs);
			Assert.Equal(_advertiseInfo.AdvertiseExternalIPAs, _settings.GossipAdvertiseInfo.AdvertiseExternalIPAs);
			Assert.Equal(_advertiseInfo.AdvertiseInternalHttpPortAs,
				_settings.GossipAdvertiseInfo.AdvertiseInternalHttpPortAs);
			Assert.Equal(_advertiseInfo.AdvertiseExternalHttpPortAs,
				_settings.GossipAdvertiseInfo.AdvertiseExternalHttpPortAs);
		}
	}

	public class with_always_keep_scavenged : SingleNodeScenario {
		public override void Given() {
			_builder.AlwaysKeepScavenged();
		}

		[Fact]
		public void should_always_keep_scavenged() {
			Assert.True(_settings.AlwaysKeepScavenged);
		}
	}

	public class with_connection_pending_send_bytes_threshold : SingleNodeScenario {
		private int _threshold = 40 * 1024;

		public override void Given() {
			_builder.WithConnectionPendingSendBytesThreshold(_threshold);
		}

		[Fact]
		public void should_set_connection_pending_send_bytes_threshold() {
			Assert.Equal(_threshold, _settings.ConnectionPendingSendBytesThreshold);
		}
	}
	
	public class with_connection_queue_size_threshold : SingleNodeScenario {
		private int _threshold = 2000;

		public override void Given() {
			_builder.WithConnectionQueueSizeThreshold(_threshold);
		}

		[Fact]
		public void should_set_connection_queue_size_threshold() {
			Assert.Equal(_threshold, _settings.ConnectionQueueSizeThreshold);
		}
	}
}
