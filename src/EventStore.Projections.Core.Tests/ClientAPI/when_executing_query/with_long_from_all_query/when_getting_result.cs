using System;
using System.Threading.Tasks;
using EventStore.Core.Tests;
using Xunit;

namespace EventStore.Projections.Core.Tests.ClientAPI.query_result.with_long_from_all_query {
	public class when_getting_result : specification_with_standard_projections_runnning {
		protected override async Task Given() {
			await base.Given();

			await PostEvent("stream-1", "type1", "{}");
			await PostEvent("stream-1", "type1", "{}");
			await PostEvent("stream-1", "type1", "{}");

			WaitIdle();
		}

		[DebugFact, Trait("Category", "Network")]
		public async Task waits_for_results() {
			const string query = @"
fromAll().when({
    $init: function(){return {count:0}},
    type1: function(s,e){
        var start = new Date();
        while(new Date()-start < 500){}
        
        s.count++;
    },
});
";

			var result = await _queryManager
				.ExecuteAsync("query", query, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(5000), _admin)
;
			Assert.Equal("{\"count\":3}", result);
		}
	}
}
