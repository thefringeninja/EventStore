﻿using System.Threading.Tasks;
using Xunit;

namespace EventStore.Projections.Core.Tests.ClientAPI.when_handling_deleted.with_from_all_foreach_projection {
	public class when_running_and_no_indexing_and_other_events : specification_with_standard_projections_runnning {
		protected override bool GivenStandardProjectionsRunning() {
			return false;
		}

		protected override async Task Given() {
			await base.Given();
			await PostEvent("stream-1", "type1", "{}");
			await PostEvent("stream-1", "type2", "{}");
			await PostEvent("stream-2", "type1", "{}");
			await PostEvent("stream-2", "type2", "{}");
			WaitIdle();
			await PostProjection(@"
fromAll().foreachStream().when({
    $init: function(){return {a:0}},
    type1: function(s,e){s.a++},
    type2: function(s,e){s.a++},
    $deleted: function(s,e){s.deleted=1;},
}).outputState();
");
		}

		protected override async Task When() {
			await base.When();
			await HardDeleteStream("stream-1");
			WaitIdle();
			await PostEvent("stream-2", "type1", "{}");
			await PostEvent("stream-2", "type2", "{}");
			await PostEvent("stream-3", "type1", "{}");
			WaitIdle();
		}

		[Fact, Trait("Category", "Network")]
		public async Task receives_deleted_notification() {
			await AssertStreamTail(
				"$projections-test-projection-stream-1-result", "Result:{\"a\":2}", "Result:{\"a\":2,\"deleted\":1}");
			await AssertStreamTail("$projections-test-projection-stream-2-result", "Result:{\"a\":4}");
			await AssertStreamTail("$projections-test-projection-stream-3-result", "Result:{\"a\":1}");
		}
	}
}
