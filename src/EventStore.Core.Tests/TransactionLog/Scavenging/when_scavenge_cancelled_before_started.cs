using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Chunks;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging {
	public class when_scavenge_cancelled_before_started : ScavengeLifeCycleScenario {
		protected override async Task When() {
			var cancellationTokenSource = new CancellationTokenSource();
			cancellationTokenSource.Cancel();
			await TfChunkScavenger.Scavenge(false, true, 0, cancellationTokenSource.Token);
		}

		[Fact]
		public void completed_logged_with_stopped_result() {
			Assert.True(Log.Completed);
			Assert.Equal(Log.Result, ScavengeResult.Stopped);
		}

		[Fact]
		public void no_chunks_scavenged() {
			Assert.Empty(Log.Scavenged);
		}

		[Fact]
		public void doesnt_call_scavenge_on_the_table_index() {
			Assert.Equal(FakeTableIndex.ScavengeCount, 0);
		}
	}
}
