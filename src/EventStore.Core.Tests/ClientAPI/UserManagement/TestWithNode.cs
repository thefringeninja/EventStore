using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.UserManagement;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI.UserManagement {
	[Category("LongRunning"), Category("ClientAPI")]
	public class TestWithNode : SpecificationWithDirectoryPerTestFixture {
		protected MiniNode _node;
		protected UsersManager _manager;

		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName);
			await _node.Start();
			_manager = new UsersManager(new NoopLogger(), _node.ExtHttpEndPoint, TimeSpan.FromSeconds(5));
		}

		[OneTimeTearDown]
		public override Task TestFixtureTearDown() {
			_node.Shutdown();
			return base.TestFixtureTearDown();
		}


		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}
	}
}
