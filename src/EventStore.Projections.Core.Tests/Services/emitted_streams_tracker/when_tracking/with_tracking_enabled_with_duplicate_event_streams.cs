using EventStore.ClientAPI.Common.Utils;
using EventStore.ClientAPI.SystemData;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream_manager.when_tracking {
	public class with_tracking_enabled_with_duplicate_event_streams : SpecificationWithEmittedStreamsTrackerAndDeleter {
		private CountdownEvent _eventAppeared = new CountdownEvent(2);
		private UserCredentials _credentials = new UserCredentials("admin", "changeit");

		protected override TimeSpan Timeout { get; } = TimeSpan.FromSeconds(10);

		protected override async Task When() {
			var sub = await _conn.SubscribeToStreamAsync(_projectionNamesBuilder.GetEmittedStreamsName(), true, (s, evnt) => {
				_eventAppeared.Signal();
				return Task.CompletedTask;
			}, userCredentials: _credentials);

			_emittedStreamsTracker.TrackEmittedStream(new EmittedEvent[] {
				new EmittedDataEvent(
					"test_stream", Guid.NewGuid(), "type1", true,
					"data", null, CheckpointTag.FromPosition(0, 100, 50), null, null),
				new EmittedDataEvent(
					"test_stream", Guid.NewGuid(), "type1", true,
					"data", null, CheckpointTag.FromPosition(0, 100, 50), null, null)
			});

			_eventAppeared.Wait(TimeSpan.FromSeconds(5));
			sub.Unsubscribe();
		}

		[Fact]
		public async Task should_at_best_attempt_to_track_a_unique_list_of_streams() {
			var result = await _conn.ReadStreamEventsForwardAsync(_projectionNamesBuilder.GetEmittedStreamsName(), 0, 200,
				false, _credentials);
			Assert.Equal(1, result.Events.Length);
			Assert.Equal("test_stream", Helper.UTF8NoBom.GetString(result.Events[0].Event.Data));
			Assert.Equal(1, _eventAppeared.CurrentCount); //only 1 event appeared should get through
		}
	}
}
