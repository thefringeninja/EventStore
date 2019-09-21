using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;

namespace EventStore.Core.Tests.Http {
	class TestSuiteMarkerBase {
		public static MiniNode _node;
		public static IEventStoreConnection _connection;
		public static int _counter;
		private SpecificationWithDirectoryPerTestFixture _directory;

		[OneTimeSetUp]
		public async Task SetUp() {
			WebRequest.DefaultWebProxy = new WebProxy();
			_counter = 0;
			_directory = new SpecificationWithDirectoryPerTestFixture();
			await _directory.TestFixtureSetUp();
			_node = new MiniNode(_directory.PathName, skipInitializeStandardUsersCheck: false, enableTrustedAuth: true);
			await _node.Start();

			_connection = TestConnection.Create(_node.TcpEndPoint);
            await _connection.ConnectAsync();
		}

		[OneTimeTearDown]
		public Task TearDown() {
			_connection.Close();
			_node.Shutdown();
			_connection = null;
			_node = null;
			return _directory.TestFixtureTearDown();
		}
	}
}
