using System;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.v8 {
	public class when_v8_projection_loading_state : TestFixtureWithJsProjection {
		protected override void Given() {
			_projection = @"
                fromAll().when({$any: 
                    function(state, event) {
                        return state;
                    }
                });
            ";
			_state = @"{""A"":""A"",""B"":""B""}";
		}

		[Fact, Trait("Category", "v8")]
		public void the_state_is_loaded() {
			string state;
			EmittedEventEnvelope[] emittedEvents;
			_stateHandler.ProcessEvent(
				"", CheckpointTag.FromPosition(0, 20, 10), "stream1", "type1", "category", Guid.NewGuid(), 0,
				"metadata",
				@"{""x"":""y""}", out state, out emittedEvents);

			Assert.Equal(_state, state);
		}
	}
}
