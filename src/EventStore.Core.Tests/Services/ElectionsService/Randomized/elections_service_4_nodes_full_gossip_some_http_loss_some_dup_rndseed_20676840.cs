using System;
using System.Linq;
using Xunit;

namespace EventStore.Core.Tests.Services.ElectionsService.Randomized {
	public class elections_service_4_nodes_full_gossip_some_http_loss_some_dup_rndseed_20676840 {
		private RandomizedElectionsTestCase _randomCase;

		public elections_service_4_nodes_full_gossip_some_http_loss_some_dup_rndseed_20676840() {
			_randomCase = new RandomizedElectionsTestCase(ElectionParams.MaxIterationCount,
				instancesCnt: 4,
				httpLossProbability: 0.5,
				httpDupProbability: 0.3,
				httpMaxDelay: 20,
				timerMinDelay: 100,
				timerMaxDelay: 200,
				rndSeed: 20676840);

			_randomCase.Init();
		}

		[Fact]
		[Trait("Category", "Network")]
		public void should_always_arrive_at_coherent_results() {
			var success = _randomCase.Run();
			if (!success)
				_randomCase.Logger.LogMessages();
			Console.WriteLine("There were a total of {0} messages in this run.",
				_randomCase.Logger.ProcessedItems.Count());
			Assert.True(success);
		}
	}
}
