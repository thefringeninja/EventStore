using System;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.v8 {
	public class when_running_a_projection_with_created_handler : TestFixtureWithJsProjection {
		protected override void Given() {
			_projection = @"
                fromAll().foreachStream().when({
                    $created: function(s, e) {
                        log('handler-invoked');
                        log(e.streamId);
                        log(e.eventType);
                        log(e.body);
                        log(e.metadata);
                        emit('stream1', 'event1', {a:1});
                    }   
                });
            ";
			_state = @"{}";
		}

		[Fact, Trait("Category", "v8")]
		public void invokes_created_handler() {
			var e = new ResolvedEvent(
				"stream", 0, "stream", 0, false, new TFPos(1000, 900), Guid.NewGuid(), "event", true, "{}",
				"{\"m\":1}");

			EmittedEventEnvelope[] emittedEvents;
			_stateHandler.ProcessPartitionCreated(
				"partition", CheckpointTag.FromPosition(0, 10, 5), e, out emittedEvents);

			Assert.Equal(5, _logged.Count);
			Assert.Equal(@"handler-invoked", _logged[0]);
			Assert.Equal(@"stream", _logged[1]);
			Assert.Equal(@"event", _logged[2]);
			Assert.Equal(@"{}", _logged[3]);
			Assert.Equal(@"{""m"":1}", _logged[4]);
		}

		[Fact, Trait("Category", "v8")]
		public void returns_emitted_events() {
			var e = new ResolvedEvent(
				"stream", 0, "stream", 0, false, new TFPos(1000, 900), Guid.NewGuid(), "event", true, "{}",
				"{\"m\":1}");

			EmittedEventEnvelope[] emittedEvents;
			_stateHandler.ProcessPartitionCreated(
				"partition", CheckpointTag.FromPosition(0, 10, 5), e, out emittedEvents);

			Assert.NotNull(emittedEvents);
			Assert.Equal(1, emittedEvents.Length);
			Assert.Equal("stream1", emittedEvents[0].Event.StreamId);
			Assert.Equal("event1", emittedEvents[0].Event.EventType);
			Assert.Equal("{\"a\":1}", emittedEvents[0].Event.Data);
		}
	}
}
