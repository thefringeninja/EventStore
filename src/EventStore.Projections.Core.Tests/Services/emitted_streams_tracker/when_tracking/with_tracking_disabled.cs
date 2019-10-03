﻿using EventStore.ClientAPI.SystemData;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.Projections.Core.Tests.Services.emitted_streams_tracker.when_tracking {
	public class with_tracking_disabled : SpecificationWithEmittedStreamsTrackerAndDeleter {
		private CountdownEvent _eventAppeared = new CountdownEvent(1);
		private UserCredentials _credentials = new UserCredentials("admin", "changeit");

		protected override TimeSpan Timeout { get; } = TimeSpan.FromSeconds(10);

		protected override Task Given() {
			_trackEmittedStreams = false;
			return base.Given();
		}

		protected override async Task When() {
			var sub = await Connection.SubscribeToStreamAsync(_projectionNamesBuilder.GetEmittedStreamsName(), true, (s, evnt) => {
				_eventAppeared.Signal();
				return Task.CompletedTask;
			}, userCredentials: _credentials);

			_emittedStreamsTracker.TrackEmittedStream(new EmittedEvent[] {
				new EmittedDataEvent(
					"test_stream", Guid.NewGuid(), "type1", true,
					"data", null, CheckpointTag.FromPosition(0, 100, 50), null, null)
			});

			_eventAppeared.Wait(TimeSpan.FromSeconds(5));
			sub.Unsubscribe();
		}

		[Fact]
		public async Task should_write_a_stream_tracked_event() {
			var result = await Connection.ReadStreamEventsForwardAsync(_projectionNamesBuilder.GetEmittedStreamsName(), 0, 200,
				false, _credentials);
			Assert.Equal(0, result.Events.Length);
			Assert.Equal(1, _eventAppeared.CurrentCount); //no event appeared should get through
		}
	}
}
