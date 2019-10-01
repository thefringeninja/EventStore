using Xunit;
using System;
using System.Net;
using EventStore.Common.Utils;

namespace EventStore.Core.Tests.Common.VNodeBuilderTests.when_building {
	public class with_cluster_dns_name : ClusterMemberScenario {
		public override void Given() {
			_builder.WithClusterDnsName("ClusterDns");
		}

		[Fact]
		public void should_set_discover_via_dns_to_true() {
			Assert.True(_settings.DiscoverViaDns);
		}

		[Fact]
		public void should_set_cluster_dns_name() {
			Assert.Equal("ClusterDns", _settings.ClusterDns);
		}
	}

	public class with_dns_discovery_disabled_and_no_gossip_seeds {
		private Exception _caughtException;
		protected VNodeBuilder _builder;

		public with_dns_discovery_disabled_and_no_gossip_seeds() {
			_builder = TestVNodeBuilder.AsClusterMember(3)
				.RunInMemory()
				.OnDefaultEndpoints()
				.DisableDnsDiscovery();
		}

		[Fact]
		public void should_not_throw_an_exception() {
			_builder.Build();
		}
	}

	public class with_dns_discovery_disabled_and_gossip_seeds_defined : ClusterMemberScenario {
		private IPEndPoint[] _gossipSeeds;

		public override void Given() {
			var baseAddress = IPAddress.Parse("127.0.1.10");
			_gossipSeeds = new IPEndPoint[] {
				new IPEndPoint(baseAddress, 1111),
				new IPEndPoint(baseAddress, 1112)
			};
			_builder.DisableDnsDiscovery()
				.WithGossipSeeds(_gossipSeeds);
		}

		[Fact]
		public void should_set_discover_via_dns_to_false() {
			Assert.False(_settings.DiscoverViaDns);
		}

		[Fact]
		public void should_set_the_gossip_seeds() {
			Assert.Equal(_gossipSeeds, _settings.GossipSeeds);
		}
	}

	public class with_prepare_ack_count_set_higher_than_the_quorum : ClusterMemberScenario {
		public override void Given() {
			_builder.WithPrepareCount(_quorumSize + 1);
		}

		[Fact]
		public void should_set_prepare_count_to_the_given_value() {
			Assert.Equal(_quorumSize + 1, _settings.PrepareAckCount);
		}
	}

	public class with_commit_ack_count_set_higher_than_the_quorum : ClusterMemberScenario {
		public override void Given() {
			_builder.WithCommitCount(_quorumSize + 1);
		}

		[Fact]
		public void should_set_commit_count_to_the_given_value() {
			Assert.Equal(_quorumSize + 1, _settings.CommitAckCount);
		}
	}

	public class with_prepare_ack_count_set_lower_than_the_quorum : ClusterMemberScenario {
		public override void Given() {
			_builder.WithPrepareCount(_quorumSize - 1);
		}

		[Fact]
		public void should_set_prepare_count_to_the_quorum_size() {
			Assert.Equal(_quorumSize, _settings.PrepareAckCount);
		}
	}

	public class with_commit_ack_count_set_lower_than_the_quorum : ClusterMemberScenario {
		public override void Given() {
			_builder.WithCommitCount(_quorumSize - 1);
		}

		[Fact]
		public void should_set_commit_count_to_the_quorum_size() {
			Assert.Equal(_quorumSize, _settings.CommitAckCount);
		}
	}

	public class with_custom_node_priority : ClusterMemberScenario {
		public override void Given() {
			_builder.WithNodePriority(5);
		}

		[Fact]
		public void should_set_the_node_priority() {
			Assert.Equal(5, _settings.NodePriority);
		}
	}

	public class with_custom_gossip_seeds : ClusterMemberScenario {
		private IPEndPoint[] _gossipSeeds;

		public override void Given() {
			var baseIpAddress = IPAddress.Parse("127.0.1.15");
			_gossipSeeds = new IPEndPoint[] {new IPEndPoint(baseIpAddress, 2112), new IPEndPoint(baseIpAddress, 3112)};
			_builder.WithGossipSeeds(_gossipSeeds);
		}

		[Fact]
		public void should_turn_off_discovery_by_dns() {
			Assert.False(_settings.DiscoverViaDns);
		}

		[Fact]
		public void should_set_the_gossip_seeds() {
			Assert.Equal(_gossipSeeds, _settings.GossipSeeds);
		}
	}

	public class with_custom_gossip_interval : ClusterMemberScenario {
		public override void Given() {
			_builder.WithGossipInterval(TimeSpan.FromMilliseconds(1300));
		}

		[Fact]
		public void should_set_the_gossip_interval() {
			Assert.Equal(1300, _settings.GossipInterval.TotalMilliseconds);
		}
	}

	public class with_custom_gossip_allowed_time_difference : ClusterMemberScenario {
		public override void Given() {
			_builder.WithGossipAllowedTimeDifference(TimeSpan.FromMilliseconds(1300));
		}

		[Fact]
		public void should_set_the_allowed_gossip_time_difference() {
			Assert.Equal(1300, _settings.GossipAllowedTimeDifference.TotalMilliseconds);
		}
	}

	public class with_custom_gossip_timeout : ClusterMemberScenario {
		public override void Given() {
			_builder.WithGossipTimeout(TimeSpan.FromMilliseconds(1300));
		}

		[Fact]
		public void should_set_the_gossip_timeout() {
			Assert.Equal(1300, _settings.GossipTimeout.TotalMilliseconds);
		}
	}

	public class with_custom_external_ip_address_as_advertise_info : ClusterMemberScenario {
		public override void Given() {
			_builder.WithExternalTcpOn(new IPEndPoint(IPAddress.Loopback, 1113))
				.WithInternalTcpOn(new IPEndPoint(IPAddress.Loopback, 1112))
				.AdvertiseExternalIPAs(IPAddress.Parse("196.168.1.1"));
		}

		[Fact]
		public void should_set_the_custom_advertise_info_for_external() {
			Assert.Equal(new IPEndPoint(IPAddress.Parse("196.168.1.1"), 1113),
				_settings.GossipAdvertiseInfo.ExternalTcp);
			Assert.Equal(new IPEndPoint(IPAddress.Parse("196.168.1.1"), 2113),
				_settings.GossipAdvertiseInfo.ExternalHttp);
		}

		[Fact]
		public void should_set_the_loopback_address_as_advertise_info_for_internal() {
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 1112), _settings.GossipAdvertiseInfo.InternalTcp);
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 2112), _settings.GossipAdvertiseInfo.InternalHttp);
		}
	}

	public class with_0_0_0_0_as_external_ip_address_and_custom_advertise_info : ClusterMemberScenario {
		public override void Given() {
			_builder.WithExternalTcpOn(new IPEndPoint(IPAddress.Parse("0.0.0.0"), 1113))
				.WithInternalTcpOn(new IPEndPoint(IPAddress.Loopback, 1112))
				.AdvertiseExternalIPAs(IPAddress.Parse("10.0.0.1"));
		}

		[Fact]
		public void should_set_the_custom_advertise_info_for_external() {
			Assert.Equal(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 1113),
				_settings.GossipAdvertiseInfo.ExternalTcp);
			Assert.Equal(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 2113),
				_settings.GossipAdvertiseInfo.ExternalHttp);
		}

		[Fact]
		public void should_set_the_loopback_address_as_advertise_info_for_internal() {
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 1112), _settings.GossipAdvertiseInfo.InternalTcp);
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 2112), _settings.GossipAdvertiseInfo.InternalHttp);
		}
	}

	public class with_0_0_0_0_as_external_ip_address_with_no_explicit_advertise_info_set : ClusterMemberScenario {
		public override void Given() {
			_builder.WithExternalTcpOn(new IPEndPoint(IPAddress.Parse("0.0.0.0"), 1113))
				.WithInternalTcpOn(new IPEndPoint(IPAddress.Loopback, 1112));
		}

		[Fact]
		public void should_use_the_non_default_loopback_ip_as_advertise_info_for_external() {
			Assert.Equal(new IPEndPoint(IPFinder.GetNonLoopbackAddress(), 1113),
				_settings.GossipAdvertiseInfo.ExternalTcp);
			Assert.Equal(new IPEndPoint(IPFinder.GetNonLoopbackAddress(), 2113),
				_settings.GossipAdvertiseInfo.ExternalHttp);
		}

		[Fact]
		public void should_use_loopback_ip_as_advertise_info_for_internal() {
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 1112), _settings.GossipAdvertiseInfo.InternalTcp);
			Assert.Equal(new IPEndPoint(IPAddress.Loopback, 2112), _settings.GossipAdvertiseInfo.InternalHttp);
		}
	}

	public class
		with_0_0_0_0_for_internal_and_external_ips_with_advertise_info_set_for_external : ClusterMemberScenario {
		public override void Given() {
			_builder.WithExternalTcpOn(new IPEndPoint(IPAddress.Parse("0.0.0.0"), 1113))
				.WithInternalTcpOn(new IPEndPoint(IPAddress.Parse("0.0.0.0"), 1112))
				.AdvertiseExternalIPAs(IPAddress.Parse("10.0.0.1"));
		}

		[Fact]
		public void should_set_the_custom_advertise_info_for_external() {
			Assert.Equal(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 1113),
				_settings.GossipAdvertiseInfo.ExternalTcp);
			Assert.Equal(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 2113),
				_settings.GossipAdvertiseInfo.ExternalHttp);
		}

		[Fact]
		public void should_use_the_non_default_loopback_ip_as_advertise_info_for_internal() {
			Assert.Equal(new IPEndPoint(IPFinder.GetNonLoopbackAddress(), 1112),
				_settings.GossipAdvertiseInfo.InternalTcp);
			Assert.Equal(new IPEndPoint(IPFinder.GetNonLoopbackAddress(), 2112),
				_settings.GossipAdvertiseInfo.InternalHttp);
		}
	}
}
