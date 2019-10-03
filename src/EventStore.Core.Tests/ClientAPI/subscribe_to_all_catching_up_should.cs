using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Common.Log;
using EventStore.Core.Services;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using ILogger = EventStore.Common.Log.ILogger;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class subscribe_to_all_catching_up_should : IClassFixture<subscribe_to_all_catching_up_should.Fixture> { public class Fixture : SpecificationWithDirectory {
		private static readonly ILogger Log = LogManager.GetLoggerFor<subscribe_to_all_catching_up_should>();
		private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

		private MiniNode _node;
		private IEventStoreConnection _conn;

		public override async Task SetUp() {
			await base.SetUp();
			_node = new MiniNode(PathName, skipInitializeStandardUsersCheck: false);
			await _node.Start();

			_conn = BuildConnection(_node);
            await _conn.ConnectAsync();
            await _conn.SetStreamMetadataAsync("$all", -1,
				StreamMetadata.Build().SetReadRole(SystemRoles.All),
				new UserCredentials(SystemUsers.Admin, SystemUsers.DefaultAdminPassword));
		}

		public override async Task TearDown() {
			_conn.Close();
			await _node.Shutdown();
			await base.TearDown();
		}

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task call_dropped_callback_after_stop_method_call() {
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var dropped = new CountdownEvent(1);
				var subscription = store.SubscribeToAllFrom(null,
					CatchUpSubscriptionSettings.Default,
					(x, y) => Task.CompletedTask,
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped.Signal());

				Assert.False(dropped.Wait(0));
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task call_dropped_callback_when_an_error_occurs_while_processing_an_event() {
			const string stream = "call_dropped_callback_when_an_error_occurs_while_processing_an_event";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
                await store.AppendToStreamAsync(stream, ExpectedVersion.Any,
					new EventData(Guid.NewGuid(), "event", false, new byte[3], null));

				var dropped = new CountdownEvent(1);
				store.SubscribeToAllFrom(null, CatchUpSubscriptionSettings.Default,
					(x, y) => { throw new Exception("Error"); },
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped.Signal());
				Assert.True(dropped.Wait(Timeout));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task be_able_to_subscribe_to_empty_db() {
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var appeared = new ManualResetEventSlim(false);
				var dropped = new CountdownEvent(1);

				var subscription = store.SubscribeToAllFrom(null,
					CatchUpSubscriptionSettings.Default,
					(_, x) => {
						if (!SystemStreams.IsSystemStream(x.OriginalEvent.EventStreamId))
							appeared.Set();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(_, __, ___) => dropped.Signal());

				await Task.Delay(100); // give time for first pull phase
                await store.SubscribeToAllAsync(false, (s, x) => Task.CompletedTask, (s, r, e) => { });
				await Task.Delay(100);

				Assert.False(appeared.Wait(0), "Some event appeared.");
				Assert.False(dropped.Wait(0), "Subscription was dropped prematurely.");
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task read_all_existing_events_and_keep_listening_to_new_ones() {
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = new List<ResolvedEvent>();
				var appeared = new CountdownEvent(20);
				var dropped = new CountdownEvent(1);

				for (int i = 0; i < 10; ++i) {
                    await store.AppendToStreamAsync("stream-" + i.ToString(), -1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				var subscription = store.SubscribeToAllFrom(null,
					CatchUpSubscriptionSettings.Default,
					(x, y) => {
						if (!SystemStreams.IsSystemStream(y.OriginalEvent.EventStreamId)) {
							events.Add(y);
							appeared.Signal();
						}

						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped.Signal());
				for (int i = 10; i < 20; ++i) {
                    await store.AppendToStreamAsync("stream-" + i.ToString(), -1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				if (!appeared.Wait(Timeout)) {
					Assert.False(dropped.Wait(0), "Subscription was dropped prematurely.");
					throw new Exception("Could not wait for all events.");
				}

				Assert.Equal(20, events.Count);
				for (int i = 0; i < 20; ++i) {
					Assert.Equal("et-" + i.ToString(), events[i].OriginalEvent.EventType);
				}

				Assert.False(dropped.Wait(0));
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task filter_events_and_keep_listening_to_new_ones() {
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = new List<ResolvedEvent>();
				var appeared = new CountdownEvent(10);
				var dropped = new CountdownEvent(1);

				for (int i = 0; i < 10; ++i) {
                    await store.AppendToStreamAsync("stream-" + i.ToString(), -1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				var allSlice = await store.ReadAllEventsForwardAsync(Position.Start, 100, false);
				var lastEvent = allSlice.Events.Last();

				var subscription = store.SubscribeToAllFrom(lastEvent.OriginalPosition,
					CatchUpSubscriptionSettings.Default,
					(x, y) => {
						events.Add(y);
						appeared.Signal();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(x, y, z) => {
						Log.Info("Subscription dropped: {0}, {1}.", y, z);
						dropped.Signal();
					});

				for (int i = 10; i < 20; ++i) {
                    await store.AppendToStreamAsync("stream-" + i.ToString(), -1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				Log.Info("Waiting for events...");
				if (!appeared.Wait(Timeout)) {
					Assert.False(dropped.Wait(0), "Subscription was dropped prematurely.");
					throw new Exception("Could not wait for all events.");
				}

				Log.Info("Events appeared...");
				Assert.Equal(10, events.Count);
				for (int i = 0; i < 10; ++i) {
					Assert.Equal("et-" + (10 + i).ToString(), events[i].OriginalEvent.EventType);
				}

				Assert.False(dropped.Wait(0));
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));

				Assert.Equal(events.Last().OriginalPosition, subscription.LastProcessedPosition);
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task filter_events_and_work_if_nothing_was_written_after_subscription() {
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = new List<ResolvedEvent>();
				var appeared = new CountdownEvent(1);
				var dropped = new CountdownEvent(1);

				for (int i = 0; i < 10; ++i) {
                    await store.AppendToStreamAsync("stream-" + i.ToString(), -1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				var allSlice = await store.ReadAllEventsForwardAsync(Position.Start, 100, false);
				var lastEvent = allSlice.Events[allSlice.Events.Length - 2];

				var subscription = store.SubscribeToAllFrom(lastEvent.OriginalPosition,
					CatchUpSubscriptionSettings.Default,
					(x, y) => {
						events.Add(y);
						appeared.Signal();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(x, y, z) => {
						Log.Info("Subscription dropped: {0}, {1}.", y, z);
						dropped.Signal();
					});

				Log.Info("Waiting for events...");
				if (!appeared.Wait(Timeout)) {
					Assert.False(dropped.Wait(0), "Subscription was dropped prematurely.");
					throw new Exception("Could not wait for all events.");
				}

				Log.Info("Events appeared...");
				Assert.Equal(1, events.Count);
				Assert.Equal("et-9", events[0].OriginalEvent.EventType);

				Assert.False(dropped.Wait(0));
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));

				Assert.Equal(events.Last().OriginalPosition, subscription.LastProcessedPosition);
			}
		}
	}
}
