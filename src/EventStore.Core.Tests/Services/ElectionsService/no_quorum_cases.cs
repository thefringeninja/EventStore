using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Services.ElectionsService {
	public sealed class elections_service_should_stuck_with_single_node_response {
		private ElectionsServiceUnit _electionsUnit;

		public elections_service_should_stuck_with_single_node_response() {
			var clusterSettingsFactory = new ClusterSettingsFactory();
			var clusterSettings = clusterSettingsFactory.GetClusterSettings(1, 3, false);

			_electionsUnit = new ElectionsServiceUnit(clusterSettings);

			ProcessElections();
		}

		private void ProcessElections() {
			var gossipUpdate = new GossipMessage.GossipUpdated(_electionsUnit.ClusterInfo);
			_electionsUnit.Publish(gossipUpdate);

			_electionsUnit.Publish(new ElectionMessage.StartElections());

			_electionsUnit.RepublishFromPublisher();
		}

		[Fact]
		public void elections_should_time_out() {
			Assert.True(_electionsUnit.Publisher.Messages.ContainsSingle<ElectionMessage.ElectionsTimedOut>());
			Assert.True(_electionsUnit.Publisher.Messages.ContainsSingle<ElectionMessage.SendViewChangeProof>());
		}
	}

	public sealed class elections_service_should_stuck_with_single_node_response_2_iterations {
		private ElectionsServiceUnit _electionsUnit;

		public elections_service_should_stuck_with_single_node_response_2_iterations() {
			var clusterSettingsFactory = new ClusterSettingsFactory();
			var clusterSettings = clusterSettingsFactory.GetClusterSettings(1, 3, false);

			_electionsUnit = new ElectionsServiceUnit(clusterSettings);

			ProcessElections();
		}

		private void ProcessElections() {
			var gossipUpdate = new GossipMessage.GossipUpdated(_electionsUnit.ClusterInfo);
			_electionsUnit.Publish(gossipUpdate);

			_electionsUnit.Publish(new ElectionMessage.StartElections());

			_electionsUnit.RepublishFromPublisher();

			_electionsUnit.RepublishFromPublisher();
			Assert.True(
				_electionsUnit.Publisher.Messages.All(x => x is HttpMessage.SendOverHttp || x is TimerMessage.Schedule),
				"Only OverHttp or Schedule messages are expected.");

			_electionsUnit.RepublishFromPublisher();
		}

		[Fact]
		public void elections_should_time_out() {
			Assert.True(_electionsUnit.Publisher.Messages.ContainsSingle<ElectionMessage.ElectionsTimedOut>());
			Assert.True(_electionsUnit.Publisher.Messages.ContainsSingle<ElectionMessage.SendViewChangeProof>());
		}
	}

	public sealed class elections_service_should_stuck_with_single_alive_node {
		private ElectionsServiceUnit _electionsUnit;

		public elections_service_should_stuck_with_single_alive_node() {
			var clusterSettingsFactory = new ClusterSettingsFactory();
			var clusterSettings = clusterSettingsFactory.GetClusterSettings(1, 3, false);

			_electionsUnit = new ElectionsServiceUnit(clusterSettings);
			_electionsUnit.UpdateClusterMemberInfo(0, isAlive: false);
			_electionsUnit.UpdateClusterMemberInfo(2, isAlive: false);
			_electionsUnit.UpdateClusterMemberInfo(3, isAlive: false);

			ProcessElections();
		}

		private void ProcessElections() {
			var gossipUpdate = new GossipMessage.GossipUpdated(_electionsUnit.ClusterInfo);
			_electionsUnit.Publish(gossipUpdate);

			_electionsUnit.Publish(new ElectionMessage.StartElections());

			_electionsUnit.RepublishFromPublisher();

			_electionsUnit.RepublishFromPublisher();
			Assert.True(
				_electionsUnit.Publisher.Messages.All(x => x is HttpMessage.SendOverHttp || x is TimerMessage.Schedule),
				"Only OverHttp or Schedule messages are expected.");

			_electionsUnit.RepublishFromPublisher();

			_electionsUnit.RepublishFromPublisher();
			Assert.True(
				_electionsUnit.Publisher.Messages.All(x => x is HttpMessage.SendOverHttp || x is TimerMessage.Schedule),
				"Only OverHttp or Schedule messages are expected.");

			_electionsUnit.RepublishFromPublisher();
		}

		[Fact]
		public void elections_should_time_out() {
			Assert.True(_electionsUnit.Publisher.Messages.ContainsSingle<ElectionMessage.ElectionsTimedOut>());
			Assert.True(_electionsUnit.Publisher.Messages.ContainsSingle<ElectionMessage.SendViewChangeProof>());
		}
	}

	public sealed class elections_service_should_be_stuck_with_live_node_and_read_only_replica{
		private ElectionsServiceUnit _electionsUnit;

		public elections_service_should_be_stuck_with_live_node_and_read_only_replica() {
			var clusterSettingsFactory = new ClusterSettingsFactory();
			var clusterSettings = clusterSettingsFactory.GetClusterSettings(1, 4, true);

			_electionsUnit = new ElectionsServiceUnit(clusterSettings);
			_electionsUnit.UpdateClusterMemberInfo(0, isAlive: true);
			_electionsUnit.UpdateClusterMemberInfo(1, isAlive: true);
			// Kill the other two nodes in the cluster
			_electionsUnit.UpdateClusterMemberInfo(2, isAlive: false);
			_electionsUnit.UpdateClusterMemberInfo(3, isAlive: false);

			StartElections();
		}
		
		private void StartElections() {
			var gossipUpdate = new GossipMessage.GossipUpdated(_electionsUnit.ClusterInfo);
			_electionsUnit.Publish(gossipUpdate);

			_electionsUnit.Publish(new ElectionMessage.StartElections());

			_electionsUnit.RepublishFromPublisher();

			_electionsUnit.RepublishFromPublisher();
			Assert.True(_electionsUnit.Publisher.Messages.All(x => x is HttpMessage.SendOverHttp || x is TimerMessage.Schedule),
				"Only OverHttp or Schedule messages are expected.");

			_electionsUnit.RepublishFromPublisher();

			_electionsUnit.RepublishFromPublisher();
			Assert.True(_electionsUnit.Publisher.Messages.All(x => x is HttpMessage.SendOverHttp || x is TimerMessage.Schedule),
				"Only OverHttp or Schedule messages are expected.");

			_electionsUnit.RepublishFromPublisher();
		}

		[Fact]
		public void elections_should_time_out() {
			Assert.True(_electionsUnit.Publisher.Messages.ContainsSingle<ElectionMessage.ElectionsTimedOut>());
			Assert.True(_electionsUnit.Publisher.Messages.ContainsSingle<ElectionMessage.SendViewChangeProof>());
		}
	}
}
