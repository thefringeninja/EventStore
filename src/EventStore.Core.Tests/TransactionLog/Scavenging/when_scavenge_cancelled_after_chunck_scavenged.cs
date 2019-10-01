using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Chunks;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging {
	public class when_scavenge_cancelled_after_chunck_scavenged : ScavengeLifeCycleScenario {
		protected override async Task When() {
			var cancellationTokenSource = new CancellationTokenSource();

			Log.ChunkScavenged += (sender, args) => cancellationTokenSource.Cancel();
            await TfChunkScavenger.Scavenge(true, true, 0, cancellationTokenSource.Token);
		}

		[Fact]
		public void completed_logged_with_stopped_result() {
			Assert.True(Log.Completed);
			Assert.Equal(ScavengeResult.Stopped, Log.Result);
		}

		[Fact]
		public void scavenge_record_for_first_and_cancelled_chunk() {
			Assert.Equal(1, Log.Scavenged.Count);
			Assert.True(Log.Scavenged[0].Scavenged);
		}


		[Fact]
		public void doesnt_call_scavenge_on_the_table_index() {
			Assert.Equal(0, FakeTableIndex.ScavengeCount);
		}
	}
}
