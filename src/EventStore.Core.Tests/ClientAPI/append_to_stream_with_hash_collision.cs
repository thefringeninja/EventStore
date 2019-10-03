using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.HashCollisions {
	[Trait("Category", "LongRunning")]
	public class append_to_stream_with_hash_collision {
		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.To(node, TcpType.Normal);
		}

		[Fact]
		public async Task should_throw_wrong_expected_version() {
			await using var fixture = new SpecificationWithDirectoryPerTestFixture(GetType());

			var node = new MiniNode(fixture.PathName,
				inMemDb: false,
				memTableSize: 20,
				hashCollisionReadLimit: 1,
				indexBitnessVersion: EventStore.Core.Index.PTableVersions.IndexV1);

			const string stream1 = "account--696193173";
			const string stream2 = "LPN-FC002_LPK51001";
			using (var store = BuildConnection(node)) {
				await store.ConnectAsync();
				//Write event to stream 1
				Assert.Equal(0, (await store.AppendToStreamAsync(stream1, ExpectedVersion.NoStream,
					new EventData(Guid.NewGuid(), "TestEvent", true, null, null))).NextExpectedVersion);
				//Write 100 events to stream 2 which will have the same hash as stream 1.
				for (int i = 0; i < 100; i++) {
					Assert.Equal(i, (await store.AppendToStreamAsync(stream2, ExpectedVersion.Any,
						new EventData(Guid.NewGuid(), "TestEvent", true, null, null))).NextExpectedVersion);
				}
			}

			var tcpPort = node.TcpEndPoint.Port;
			var tcpSecPort = node.TcpSecEndPoint.Port;
			var httpPort = node.ExtHttpEndPoint.Port;
			await node.Shutdown(keepDb: true);

			//Restart the node to ensure the read index stream info cache is empty
			node = new MiniNode(fixture.PathName,
				tcpPort, tcpSecPort, httpPort, inMemDb: false,
				memTableSize: 20,
				hashCollisionReadLimit: 1,
				indexBitnessVersion: EventStore.Core.Index.PTableVersions.IndexV1);
			await node.Start();
			using (var store = BuildConnection(node)) {
				await store.ConnectAsync();

				await Assert.ThrowsAsync<WrongExpectedVersionException>(
					() => store.AppendToStreamAsync(stream1, ExpectedVersion.Any,
						new EventData(Guid.NewGuid(), "TestEvent", true, null, null)));
			}
		}
	}
}
