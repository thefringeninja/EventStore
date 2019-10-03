using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "LongRunning"), Trait("Category", "ClientAPI")]
	public class catchup_subscription_handles_small_batch_sizes :
		IClassFixture<catchup_subscription_handles_small_batch_sizes.Fixture> {
		private readonly Fixture _fixture;
		private readonly ITestOutputHelper _testOutputHelper;

		public class Fixture : SpecificationWithDirectoryPerTestFixture {
			private MiniNode _node;
			public string StreamName = "TestStream";
			public CatchUpSubscriptionSettings Settings;
			public IEventStoreConnection Connection;

			public override async Task TestFixtureSetUp() {
				await base.TestFixtureSetUp();
				_node = new MiniNode(PathName, inMemDb: true);
				await _node.Start();

				Connection = BuildConnection(_node);
				await Connection.ConnectAsync();
				//Create 80000 events
				for (var i = 0; i < 80; i++) {
					await Connection.AppendToStreamAsync(StreamName, ExpectedVersion.Any, CreateThousandEvents());
				}

				Settings = new CatchUpSubscriptionSettings(100, 1, false, true, String.Empty);
			}

			private EventData[] CreateThousandEvents() {
				var events = new List<EventData>();
				for (var i = 0; i < 1000; i++) {
					events.Add(new EventData(Guid.NewGuid(), "testEvent", true,
						Encoding.UTF8.GetBytes("{ \"Foo\":\"Bar\" }"), null));
				}

				return events.ToArray();
			}

			public override async Task TestFixtureTearDown() {
				Connection.Dispose();
				await _node.Shutdown();
				await base.TestFixtureTearDown();
			}

			protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
				return TestConnection.Create(node.TcpEndPoint);
			}
		}

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		public catchup_subscription_handles_small_batch_sizes(Fixture fixture, ITestOutputHelper testOutputHelper) {
			_fixture = fixture;
			_testOutputHelper = testOutputHelper;
		}

		[Fact(Skip = "Very long running")]
		public void CatchupSubscriptionToAllHandlesManyEventsWithSmallBatchSize() {
			var mre = new ManualResetEvent(false);
			_fixture.Connection.SubscribeToAllFrom(null, _fixture.Settings, (sub, evnt) => {
				if (evnt.OriginalEventNumber % 1000 == 0) {
					_testOutputHelper.WriteLine("Processed {0} events", evnt.OriginalEventNumber);
				}

				return Task.CompletedTask;
			}, (sub) => { mre.Set(); }, null, new UserCredentials("admin", "changeit"));

			Assert.True(mre.WaitOne(TimeSpan.FromMinutes(10)));
		}

		[Fact(Skip = "Very long running")]
		public void CatchupSubscriptionToStreamHandlesManyEventsWithSmallBatchSize() {
			var mre = new ManualResetEvent(false);
			_fixture.Connection.SubscribeToStreamFrom(_fixture.StreamName, null, _fixture.Settings, (sub, evnt) => {
				if (evnt.OriginalEventNumber % 1000 == 0) {
					_testOutputHelper.WriteLine("Processed {0} events", evnt.OriginalEventNumber);
				}

				return Task.CompletedTask;
			}, (sub) => { mre.Set(); }, null, new UserCredentials("admin", "changeit"));

			Assert.True(mre.WaitOne(TimeSpan.FromMinutes(10)));
		}
	}
}
