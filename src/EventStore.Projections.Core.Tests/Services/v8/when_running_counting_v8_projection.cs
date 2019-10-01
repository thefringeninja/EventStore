using System;
using System.Globalization;
using EventStore.Core.Tests;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.v8 {
	public class when_running_counting_v8_projection : TestFixtureWithJsProjection {
		protected override void Given() {
			_projection = @"
                fromAll().when({$any: 
                    function(state, event) {
                        state.count = state.count + 1;
                        log(state.count);
                        return state;
                    }});
            ";
			_state = @"{""count"": 0}";
		}

		[Fact, Trait("Category", "v8")]
		public void process_event_counts_events() {
			string state;
			EmittedEventEnvelope[] emittedEvents;
			_stateHandler.ProcessEvent(
				"", CheckpointTag.FromPosition(0, 10, 5), "stream1", "type1", "category", Guid.NewGuid(), 0, "metadata",
				@"{""a"":""b""}", out state, out emittedEvents);
			_stateHandler.ProcessEvent(
				"", CheckpointTag.FromPosition(0, 20, 15), "stream1", "type1", "category", Guid.NewGuid(), 1,
				"metadata",
				@"{""a"":""b""}", out state, out emittedEvents);
			Assert.Equal(2, _logged.Count);
			Assert.Equal(@"1", _logged[0]);
			Assert.Equal(@"2", _logged[1]);
		}

		[Explicit, Trait("Category", "v8"), Trait("Category", "Manual")]
		public void can_handle_million_events() {
			for (var i = 0; i < 1000000; i++) {
				_logged.Clear();
				string state;
				EmittedEventEnvelope[] emittedEvents;
				_stateHandler.ProcessEvent(
					"", CheckpointTag.FromPosition(0, i * 10, i * 10 - 5), "stream" + i, "type" + i, "category",
					Guid.NewGuid(), 0,
					"metadata", @"{""a"":""" + i + @"""}", out state, out emittedEvents);
				Assert.Equal(1, _logged.Count);
				Assert.Equal((i + 1).ToString(CultureInfo.InvariantCulture), _logged[_logged.Count - 1]);
			}
		}
	}
}
