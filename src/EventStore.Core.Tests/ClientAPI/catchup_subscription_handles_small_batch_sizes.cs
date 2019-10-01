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

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "LongRunning"), Trait("Category", "ClientAPI")]
	public class catchup_subscription_handles_small_batch_sizes : SpecificationWithDirectoryPerTestFixture {
		private MiniNode _node;
		private string _streamName = "TestStream";
		private CatchUpSubscriptionSettings _settings;
		private IEventStoreConnection _conn;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName, inMemDb: true);
			await _node.Start();

			_conn = BuildConnection(_node);
			await _conn.ConnectAsync();
			//Create 80000 events
			for (var i = 0; i < 80; i++) {
				await _conn.AppendToStreamAsync(_streamName, ExpectedVersion.Any, CreateThousandEvents());
			}

			_settings = new CatchUpSubscriptionSettings(100, 1, false, true, String.Empty);
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
			_conn.Dispose();
			await _node.Shutdown();
			await base.TestFixtureTearDown();
		}

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		[Fact(Skip = "Very long running")]
		public void CatchupSubscriptionToAllHandlesManyEventsWithSmallBatchSize() {
			var mre = new ManualResetEvent(false);
			_conn.SubscribeToAllFrom(null, _settings, (sub, evnt) => {
				if (evnt.OriginalEventNumber % 1000 == 0) {
					Console.WriteLine("Processed {0} events", evnt.OriginalEventNumber);
				}

				return Task.CompletedTask;
			}, (sub) => { mre.Set(); }, null, new UserCredentials("admin", "changeit"));

			Assert.True(mre.WaitOne(TimeSpan.FromMinutes(10)));
		}

		[Fact(Skip = "Very long running")]
		public void CatchupSubscriptionToStreamHandlesManyEventsWithSmallBatchSize() {
			var mre = new ManualResetEvent(false);
			_conn.SubscribeToStreamFrom(_streamName, null, _settings, (sub, evnt) => {
				if (evnt.OriginalEventNumber % 1000 == 0) {
					Console.WriteLine("Processed {0} events", evnt.OriginalEventNumber);
				}

				return Task.CompletedTask;
			}, (sub) => { mre.Set(); }, null, new UserCredentials("admin", "changeit"));

			Assert.True(mre.WaitOne(TimeSpan.FromMinutes(10)));
		}
	}
}
