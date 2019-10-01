using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Chunks;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging {
	public class when_scavenge_cancelled_after_completed : ScavengeLifeCycleScenario {
		protected override async Task When() {
			var cancellationTokenSource = new CancellationTokenSource();

			Log.CompletedCallback += (sender, args) => cancellationTokenSource.Cancel();
            await TfChunkScavenger.Scavenge(true, true, 0, cancellationTokenSource.Token);
		}

		[Fact]
		public void completed_logged_with_success_result() {
			Assert.True(Log.Completed);
			Assert.Equal(Log.Result, ScavengeResult.Success);
		}

		[Fact]
		public void scavenge_record_for_all_completed_chunks() {
			Assert.Equal(2, Log.Scavenged.Count);
			Assert.True(Log.Scavenged[0].Scavenged);
			Assert.True(Log.Scavenged[1].Scavenged);
		}


		[Fact]
		public void merge_record_for_all_completed_merged() {
			Assert.Single(Log.Merged);
			Assert.True(Log.Merged[0].Scavenged);;
		}


		[Fact]
		public void calls_scavenge_on_the_table_index() {
			Assert.Equal(FakeTableIndex.ScavengeCount, 1);
		}
	}
}
