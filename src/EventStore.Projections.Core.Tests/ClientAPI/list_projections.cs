using System;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Tests;
using Xunit;

namespace EventStore.Projections.Core.Tests.ClientAPI {
	public class list_projections : specification_with_standard_projections_runnning {
		const string TestProjection =
			"fromAll().when({$init: function (state, ev) {return {};},ConversationStarted: function (state, ev) {state.lastBatchSent = ev;return state;}});";

		[DebugFact]
		public async Task list_all_projections_works() {
			var x = await _manager.ListAllAsync(new UserCredentials("admin", "changeit"));
			Assert.Equal(true, x.Any());
			Assert.True(x.Any(p => p.Name == "$streams"));
		}

		[DebugFact]
		public async Task  list_oneTime_projections_works() {
            await _manager.CreateOneTimeAsync(TestProjection, new UserCredentials("admin", "changeit"));
			var x = await _manager.ListOneTimeAsync(new UserCredentials("admin", "changeit"));
			Assert.Equal(true, x.Any(p => p.Mode == "OneTime"));
		}

		[DebugFact]
		public async Task list_continuous_projections_works() {
			var nameToTest = Guid.NewGuid().ToString();
            await _manager.CreateContinuousAsync(nameToTest, TestProjection, new UserCredentials("admin", "changeit"));
			var x = await _manager.ListContinuousAsync(new UserCredentials("admin", "changeit"));
			Assert.Equal(true, x.Any(p => p.Name == nameToTest));
		}
	}
}
