using System;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.v8 {
	public class when_not_returning_state_from_a_js_handler : TestFixtureWithJsProjection {
		protected override void Given() {
			_projection = @"
                fromAll().when({$any: function(state, event) {
                    state.newValue = 'new';
                }});
            ";
		}

		[Fact, Trait("Category", "v8")]
		public void process_event_should_return_updated_state() {
			string state;
			EmittedEventEnvelope[] emittedEvents;
			_stateHandler.ProcessEvent(
				"", CheckpointTag.FromPosition(0, 20, 10), "stream1", "type1", "category",
				Guid.NewGuid(), 0, "metadata", @"{""a"":""b""}", out state, out emittedEvents);
			Assert.True(state.Contains("\"newValue\":\"new\""));
		}

		[Fact, Trait("Category", "v8")]
		public void process_event_returns_true() {
			string state;
			EmittedEventEnvelope[] emittedEvents;
			var result = _stateHandler.ProcessEvent(
				"", CheckpointTag.FromPosition(0, 20, 10), "stream1", "type1", "category",
				Guid.NewGuid(), 0, "metadata", @"{""a"":""b""}", out state, out emittedEvents);

			Assert.True(result);
		}
	}
}
