using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace EventStore.Client.Streams {
	[Trait("Category", "LongRunning")]
	public class subscribe_to_stream_with_revision : IClassFixture<subscribe_to_stream_with_revision.Fixture>, IDisposable {
		private readonly Fixture _fixture;
		private readonly IDisposable _loggingContext;

		public subscribe_to_stream_with_revision(Fixture fixture, ITestOutputHelper outputHelper) {
			_fixture = fixture;
			_loggingContext = LoggingHelper.Capture(outputHelper);
		}

		[Fact]
		public async Task subscribe_to_non_existing_stream() {
			var stream = _fixture.GetStreamName();
			var appeared = new TaskCompletionSource<bool>();
			var dropped = new TaskCompletionSource<(SubscriptionDroppedReason, Exception)>();

			using var subscription = _fixture.Client.SubscribeToStream(stream, StreamRevision.Start, EventAppeared,
				false, SubscriptionDropped);

			await Task.Delay(200);

			Assert.False(appeared.Task.IsCompleted);

			if (dropped.Task.IsCompleted) {
				Assert.False(dropped.Task.IsCompleted, dropped.Task.Result.ToString());
			}

			subscription.Dispose();

			var (reason, ex) = await dropped.Task.WithTimeout();
			Assert.Equal(SubscriptionDroppedReason.Disposed, reason);
			Assert.Null(ex);

			Task EventAppeared(StreamSubscription s, ResolvedEvent e, CancellationToken ct) {
				appeared.TrySetResult(true);
				return Task.CompletedTask;
			}

			void SubscriptionDropped(StreamSubscription s, SubscriptionDroppedReason reason, Exception ex)
				=> dropped.SetResult((reason, ex));
		}

		[Fact]
		public async Task subscribe_to_non_existing_stream_then_get_event() {
			var stream = _fixture.GetStreamName();
			var appeared = new TaskCompletionSource<bool>();
			var dropped = new TaskCompletionSource<(SubscriptionDroppedReason, Exception)>();

			using var subscription = _fixture.Client.SubscribeToStream(stream, StreamRevision.Start, EventAppeared,
				false, SubscriptionDropped);

			await _fixture.Client.AppendToStreamAsync(stream, AnyStreamRevision.NoStream, _fixture.CreateTestEvents(40));

			Assert.True(await appeared.Task.WithTimeout());

			if (dropped.Task.IsCompleted) {
				Assert.False(dropped.Task.IsCompleted, dropped.Task.Result.ToString());
			}

			subscription.Dispose();

			var (reason, ex) = await dropped.Task.WithTimeout();
			Assert.Equal(SubscriptionDroppedReason.Disposed, reason);
			Assert.Null(ex);

			Task EventAppeared(StreamSubscription s, ResolvedEvent e, CancellationToken ct) {
				if (e.OriginalEvent.EventNumber == StreamRevision.Start) {
					appeared.TrySetException(new Exception());
				} else {
					appeared.TrySetResult(true);
				}

				return Task.CompletedTask;
			}

			void SubscriptionDropped(StreamSubscription s, SubscriptionDroppedReason reason, Exception ex)
				=> dropped.SetResult((reason, ex));
		}

		[Fact]
		public async Task allow_multiple_subscriptions_to_same_stream() {
			var stream = _fixture.GetStreamName();

			var appeared = new TaskCompletionSource<bool>();

			int appearedCount = 0;

			using var s1 = _fixture.Client.SubscribeToStream(stream, StreamRevision.Start, EventAppeared);
			using var s2 = _fixture.Client.SubscribeToStream(stream, StreamRevision.Start, EventAppeared);
			await _fixture.Client.AppendToStreamAsync(stream, AnyStreamRevision.NoStream, _fixture.CreateTestEvents(2));

			Assert.True(await appeared.Task.WithTimeout());

			Task EventAppeared(StreamSubscription s, ResolvedEvent e, CancellationToken ct) {
				if (e.OriginalEvent.EventNumber == StreamRevision.Start) {
					appeared.TrySetException(new Exception());
					return Task.CompletedTask;
				}

				if (++appearedCount == 2) {
					appeared.TrySetResult(true);
				}

				return Task.CompletedTask;
			}
		}

		[Fact]
		public async Task calls_subscription_dropped_when_disposed() {
			var stream = _fixture.GetStreamName();
			var dropped = new TaskCompletionSource<(SubscriptionDroppedReason, Exception)>();

			using var subscription = _fixture.Client.SubscribeToStream(stream, StreamRevision.Start, EventAppeared,
				false, SubscriptionDropped);

			if (dropped.Task.IsCompleted) {
				Assert.False(dropped.Task.IsCompleted, dropped.Task.Result.ToString());
			}

			subscription.Dispose();

			var (reason, ex) = await dropped.Task.WithTimeout();

			Assert.Equal(SubscriptionDroppedReason.Disposed, reason);
			Assert.Null(ex);

			Task EventAppeared(StreamSubscription s, ResolvedEvent e, CancellationToken ct) => Task.CompletedTask;

			void SubscriptionDropped(StreamSubscription s, SubscriptionDroppedReason reason, Exception ex) =>
				dropped.SetResult((reason, ex));
		}

		[Fact]
		public async Task calls_subscription_dropped_when_error_processing_event() {
			var stream = _fixture.GetStreamName();
			var dropped = new TaskCompletionSource<(SubscriptionDroppedReason, Exception)>();
			var expectedException = new Exception("Error");

			using var subscription = _fixture.Client.SubscribeToStream(stream, StreamRevision.Start, EventAppeared,
				false, SubscriptionDropped);

			if (dropped.Task.IsCompleted) {
				Assert.False(dropped.Task.IsCompleted, dropped.Task.Result.ToString());
			}

			await _fixture.Client.AppendToStreamAsync(stream, AnyStreamRevision.NoStream, _fixture.CreateTestEvents(2));

			var (reason, ex) = await dropped.Task.WithTimeout();

			Assert.Equal(SubscriptionDroppedReason.SubscriberError, reason);
			Assert.Same(expectedException, ex);

			Task EventAppeared(StreamSubscription s, ResolvedEvent e, CancellationToken ct) =>
				Task.FromException(expectedException);

			void SubscriptionDropped(StreamSubscription s, SubscriptionDroppedReason reason, Exception ex) =>
				dropped.SetResult((reason, ex));
		}

		[Fact]
		public async Task reads_all_existing_events_and_keep_listening_to_new_ones() {
			var stream = _fixture.GetStreamName();
			var appeared = new TaskCompletionSource<bool>();
			var dropped = new TaskCompletionSource<(SubscriptionDroppedReason, Exception)>();
			var appearedEvents = new List<EventRecord>();
			var beforeEvents = _fixture.CreateTestEvents(10).ToArray();
			var afterEvents = _fixture.CreateTestEvents(10).ToArray();

			await _fixture.Client.AppendToStreamAsync(stream, AnyStreamRevision.NoStream, _fixture.CreateTestEvents());

			await _fixture.Client.AppendToStreamAsync(stream, AnyStreamRevision.Any, beforeEvents);

			using var subscription =
				_fixture.Client.SubscribeToStream(stream, StreamRevision.Start, EventAppeared, false, SubscriptionDropped);

			var writeResult = await _fixture.Client.AppendToStreamAsync(stream, AnyStreamRevision.Any, afterEvents);

			await appeared.Task.WithTimeout();

			AssertEx.EventsEqual(beforeEvents.Concat(afterEvents).ToArray(), appearedEvents.ToArray());

			if (dropped.Task.IsCompleted) {
				Assert.False(dropped.Task.IsCompleted, dropped.Task.Result.ToString());
			}

			subscription.Dispose();

			var (reason, ex) = await dropped.Task.WithTimeout();

			Assert.Equal(SubscriptionDroppedReason.Disposed, reason);
			Assert.Null(ex);

			Task EventAppeared(StreamSubscription s, ResolvedEvent e, CancellationToken ct) {
				if (e.OriginalEvent.EventNumber == StreamRevision.Start) {
					appeared.TrySetException(new Exception());
					return Task.CompletedTask;
				}

				appearedEvents.Add(e.Event);

				if (appearedEvents.Count >= beforeEvents.Length + afterEvents.Length) {
					appeared.TrySetResult(true);
				}

				return Task.CompletedTask;
			}

			void SubscriptionDropped(StreamSubscription s, SubscriptionDroppedReason reason, Exception ex) {
				dropped.SetResult((reason, ex));
			}
		}


		public class Fixture : EventStoreGrpcFixture {
			public EventData[] Events { get; }

			public Fixture() {
				Events = CreateTestEvents(10).ToArray();
			}

			protected override Task Given() => Task.CompletedTask;

			protected override Task When() => Task.CompletedTask;
		}

		public void Dispose() => _loggingContext.Dispose();
	}
}
