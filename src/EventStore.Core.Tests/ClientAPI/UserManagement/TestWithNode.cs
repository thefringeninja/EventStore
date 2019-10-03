using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.UserManagement;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement {
	[Trait("Category", "LongRunning"), Trait("Category", "ClientAPI")]
	public class TestWithNode : IClassFixture<TestWithNode.Fixture> { public class Fixture : SpecificationWithDirectoryPerTestFixture {
		protected MiniNode _node;
		protected UsersManager _manager;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName);
			await _node.Start();
			_manager = new UsersManager(new NoopLogger(), _node.ExtHttpEndPoint, TimeSpan.FromSeconds(5));
		}

		public override async Task TestFixtureTearDown() {
			await _node.Shutdown();
			await base.TestFixtureTearDown();
		}


		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}
	}
}
