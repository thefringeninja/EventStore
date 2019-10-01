using System;
using EventStore.Core.Tests;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.v8 {
	public class when_running_emitting_v8_projection : TestFixtureWithJsProjection {
		protected override void Given() {
			_projection = @"
                fromAll().when({$any: 
                    function(state, event) {
                    emit('output-stream' + event.sequenceNumber, 'emitted-event' + event.sequenceNumber, {a: JSON.parse(event.bodyRaw).a});
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
			Assert.Equal("emitted-event0", emittedEvents[0].Event.EventType);
			Assert.Equal("output-stream0", emittedEvents[0].Event.StreamId);
			Assert.Equal(@"{""a"":""b""}", emittedEvents[0].Event.Data);
		}

		[Explicit, Trait("Category", "v8"), Trait("Category", "Manual")]
		public void can_pass_though_millions_of_events() {
			for (var i = 0; i < 100000000; i++) {
				string state;
				EmittedEventEnvelope[] emittedEvents;
				_stateHandler.ProcessEvent(
					"", CheckpointTag.FromPosition(0, i * 10 + 20, i * 10 + 10), "stream" + i, "type" + i, "category",
					Guid.NewGuid(), i,
					"metadata", @"{""a"":""" + i + @"""}", out state, out emittedEvents);

				Assert.NotNull(emittedEvents);
				Assert.Equal(1, emittedEvents.Length);
				Assert.Equal("emitted-event" + i, emittedEvents[0].Event.EventType);
				Assert.Equal("output-stream" + i, emittedEvents[0].Event.StreamId);
				Assert.Equal(@"{""a"":""" + i + @"""}", emittedEvents[0].Event.Data);

				if (i % 10000 == 0) {
					Teardown();
					Setup(); // recompile..
					Console.Write(".");
				}
			}
		}
	}
}
