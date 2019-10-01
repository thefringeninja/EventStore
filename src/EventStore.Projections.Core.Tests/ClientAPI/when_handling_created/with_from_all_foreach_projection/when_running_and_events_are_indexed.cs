using System.Threading.Tasks;
using EventStore.Core.Tests;
using Xunit;

namespace EventStore.Projections.Core.Tests.ClientAPI.when_handling_created.with_from_all_foreach_projection {
	public class when_running_and_events_are_indexed : specification_with_standard_projections_runnning {
		protected override bool GivenStandardProjectionsRunning() {
			return false;
		}

		protected override async Task Given() {
			await base.Given();
			await PostEvent("stream-1", "type1", "{}");
			await PostEvent("stream-1", "type2", "{}");
			await PostEvent("stream-2", "type1", "{}");
			await PostEvent("stream-2", "type2", "{}");
		}

		protected override async Task When() {
			await base.When();
			await PostProjection(@"
fromAll().foreachStream().when({
    $init: function(){return {a:0}},
    type1: function(s,e){s.a++;},
    type2: function(s,e){s.a++;},
    $created: function(s,e){s.a++;},
}).outputState();
");
			WaitIdle();
		}

		[DebugFact, Trait("Category", "Network")]
		public async Task receives_deleted_notification() {
			await AssertStreamTail("$projections-test-projection-stream-1-result", "Result:{\"a\":3}");
			await AssertStreamTail("$projections-test-projection-stream-2-result", "Result:{\"a\":3}");
		}
	}
}
