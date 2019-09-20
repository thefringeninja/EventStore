using System.Net;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI {
	public abstract class SpecificationWithMiniNode : SpecificationWithDirectoryPerTestFixture {
		protected MiniNode _node;
		protected IEventStoreConnection _conn;
		protected IPEndPoint _HttpEndPoint;

		protected virtual void Given() {
		}

		protected abstract void When();

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName, skipInitializeStandardUsersCheck: false);
			_node.Start();
			_HttpEndPoint = _node.ExtHttpEndPoint;
			_conn = BuildConnection(_node);
			await _conn.ConnectAsync();
			Given();
			When();
		}

		[OneTimeTearDown]
		public override Task TestFixtureTearDown() {
			_conn.Close();
			_node.Shutdown();
			return base.TestFixtureTearDown();
		}
	}
}
