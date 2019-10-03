using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Common.Log;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class subscribe_to_stream_catching_up_should : IClassFixture<subscribe_to_stream_catching_up_should.Fixture> { public class Fixture : SpecificationWithDirectoryPerTestFixture {
		private static readonly EventStore.Common.Log.ILogger Log =
			LogManager.GetLoggerFor<subscribe_to_stream_catching_up_should>();

		private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(500);

		private MiniNode _node;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName);
			await _node.Start();
		}

		public override async Task TestFixtureTearDown() {
			await _node.Shutdown();
			await base.TestFixtureTearDown();
		}

		virtual protected IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task be_able_to_subscribe_to_non_existing_stream() {
			const string stream = "be_able_to_subscribe_to_non_existing_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var appeared = new ManualResetEventSlim(false);
				var dropped = new CountdownEvent(1);

				var subscription = store.SubscribeToStreamFrom(stream,
					null,
					CatchUpSubscriptionSettings.Default,
					(_, x) => {
						appeared.Set();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(_, __, ___) => dropped.Signal());

				await Task.Delay(100); // give time for first pull phase
                await store.SubscribeToStreamAsync(stream, false, (s, x) => Task.CompletedTask, (s, r, e) => { });
				await Task.Delay(100);
				Assert.False(appeared.Wait(0), "Some event appeared.");
				Assert.False(dropped.Wait(0), "Subscription was dropped prematurely.");
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task be_able_to_subscribe_to_non_existing_stream_and_then_catch_event() {
			const string stream = "be_able_to_subscribe_to_non_existing_stream_and_then_catch_event";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var appeared = new CountdownEvent(1);
				var dropped = new CountdownEvent(1);

				var subscription = store.SubscribeToStreamFrom(stream,
					null,
					CatchUpSubscriptionSettings.Default,
					(_, x) => {
						appeared.Signal();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(_, __, ___) => dropped.Signal());

                await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent());

				if (!appeared.Wait(Timeout)) {
					Assert.False(dropped.Wait(0), "Subscription was dropped prematurely.");
					throw new Exception("Appeared countdown event timed out.");
				}

				Assert.False(dropped.Wait(0));
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task allow_multiple_subscriptions_to_same_stream() {
			const string stream = "allow_multiple_subscriptions_to_same_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var appeared = new CountdownEvent(2);
				var dropped1 = new ManualResetEventSlim(false);
				var dropped2 = new ManualResetEventSlim(false);

				var sub1 = store.SubscribeToStreamFrom(stream,
					null,
					CatchUpSubscriptionSettings.Default,
					(_, e) => {
						appeared.Signal();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped1.Set());
				var sub2 = store.SubscribeToStreamFrom(stream,
					null,
					CatchUpSubscriptionSettings.Default,
					(_, e) => {
						appeared.Signal();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped2.Set());

                await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent());

				if (!appeared.Wait(Timeout)) {
					Assert.False(dropped1.Wait(0), "Subscription1 was dropped prematurely.");
					Assert.False(dropped2.Wait(0), "Subscription2 was dropped prematurely.");
					throw new Exception("Could not wait for all events.");
				}

				Assert.False(dropped1.Wait(0));
				sub1.Stop(Timeout);
				Assert.True(dropped1.Wait(Timeout));

				Assert.False(dropped2.Wait(0));
				sub2.Stop(Timeout);
				Assert.True(dropped2.Wait(Timeout));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task call_dropped_callback_after_stop_method_call() {
			const string stream = "call_dropped_callback_after_stop_method_call";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var dropped = new CountdownEvent(1);
				var subscription = store.SubscribeToStreamFrom(stream,
					null,
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
				store.SubscribeToStreamFrom(stream, null,
					CatchUpSubscriptionSettings.Default,
					(x, y) => { throw new Exception("Error"); },
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped.Signal());
				Assert.True(dropped.Wait(Timeout));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task read_all_existing_events_and_keep_listening_to_new_ones() {
			const string stream = "read_all_existing_events_and_keep_listening_to_new_ones";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = new List<ResolvedEvent>();
				var appeared = new CountdownEvent(20); // events
				var dropped = new CountdownEvent(1);

				for (int i = 0; i < 10; ++i) {
                    await store.AppendToStreamAsync(stream, i - 1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				var subscription = store.SubscribeToStreamFrom(stream,
					null,
					CatchUpSubscriptionSettings.Default,
					(x, y) => {
						events.Add(y);
						appeared.Signal();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped.Signal());
				for (int i = 10; i < 20; ++i) {
                    await store.AppendToStreamAsync(stream, i - 1,
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
			const string stream = "filter_events_and_keep_listening_to_new_ones";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = new List<ResolvedEvent>();
				var appeared = new CountdownEvent(20); // skip first 10 events
				var dropped = new CountdownEvent(1);

				for (int i = 0; i < 20; ++i) {
                    await store.AppendToStreamAsync(stream, i - 1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				var subscription = store.SubscribeToStreamFrom(stream,
					9,
					CatchUpSubscriptionSettings.Default,
					(x, y) => {
						events.Add(y);
						appeared.Signal();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped.Signal());
				for (int i = 20; i < 30; ++i) {
                    await store.AppendToStreamAsync(stream, i - 1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				if (!appeared.Wait(Timeout)) {
					Assert.False(dropped.Wait(0), "Subscription was dropped prematurely.");
					throw new Exception("Could not wait for all events.");
				}

				Assert.Equal(20, events.Count);
				for (int i = 0; i < 20; ++i) {
					Assert.Equal("et-" + (i + 10).ToString(), events[i].OriginalEvent.EventType);
				}

				Assert.False(dropped.Wait(0));
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));

				Assert.Equal(events.Last().OriginalEventNumber, subscription.LastProcessedEventNumber);

				subscription.Stop(TimeSpan.FromSeconds(0));
			}
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task filter_events_and_work_if_nothing_was_written_after_subscription() {
			const string stream = "filter_events_and_work_if_nothing_was_written_after_subscription";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = new List<ResolvedEvent>();
				var appeared = new CountdownEvent(10);
				var dropped = new CountdownEvent(1);

				for (int i = 0; i < 20; ++i) {
                    await store.AppendToStreamAsync(stream, i - 1,
						new EventData(Guid.NewGuid(), "et-" + i.ToString(), false, new byte[3], null));
				}

				var subscription = store.SubscribeToStreamFrom(stream,
					9,
					CatchUpSubscriptionSettings.Default,
					(x, y) => {
						events.Add(y);
						appeared.Signal();
						return Task.CompletedTask;
					},
					_ => Log.Info("Live processing started."),
					(x, y, z) => dropped.Signal());
				if (!appeared.Wait(Timeout)) {
					Assert.False(dropped.Wait(0), "Subscription was dropped prematurely.");
					throw new Exception("Could not wait for all events.");
				}

				Assert.Equal(10, events.Count);
				for (int i = 0; i < 10; ++i) {
					Assert.Equal("et-" + (i + 10).ToString(), events[i].OriginalEvent.EventType);
				}

				Assert.False(dropped.Wait(0));
				subscription.Stop(Timeout);
				Assert.True(dropped.Wait(Timeout));

				Assert.Equal(events.Last().OriginalEventNumber, subscription.LastProcessedEventNumber);
			}
		}
	}
}
