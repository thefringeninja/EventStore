using System;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.v8 {
	public class when_running_a_v8_projection_emitting_stream_links : TestFixtureWithJsProjection {
		protected override void Given() {
			_projection = @"
                fromAll().when({$any: 
                    function(state, event) {
                    linkStreamTo('output-stream' + event.sequenceNumber, 'stream' + event.sequenceNumber);
                    return {};
                }});
            ";
		}

		[Fact, Trait("Category", "v8")]
		public void process_event_returns_true() {
			string state;
			EmittedEventEnvelope[] emittedEvents;
			var result = _stateHandler.ProcessEvent(
				"", CheckpointTag.FromPosition(0, 20, 10), "stream1", "type1", "category", Guid.NewGuid(), 0,
				"metadata",
				@"{""a"":""b""}", out state, out emittedEvents);

			Assert.True(result);
		}

		[Fact, Trait("Category", "v8")]
		public void process_event_returns_emitted_event() {
			string state;
			EmittedEventEnvelope[] emittedEvents;
			_stateHandler.ProcessEvent(
				"", CheckpointTag.FromPosition(0, 20, 10), "stream1", "type1", "category", Guid.NewGuid(), 0,
				"metadata",
				@"{""a"":""b""}", out state, out emittedEvents);

			Assert.NotNull(emittedEvents);
			Assert.Equal(1, emittedEvents.Length);
			Assert.Equal("$@", emittedEvents[0].Event.EventType);
			Assert.Equal("output-stream0", emittedEvents[0].Event.StreamId);
			Assert.Equal("stream0", emittedEvents[0].Event.Data);
		}
	}
}
