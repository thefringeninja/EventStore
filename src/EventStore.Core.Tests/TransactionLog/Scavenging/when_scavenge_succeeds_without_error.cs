using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Chunks;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging {
	public class when_scavenge_succeeds_without_error : ScavengeLifeCycleScenario {
		protected override Task When() {
			var cancellationTokenSource = new CancellationTokenSource();
			return TfChunkScavenger.Scavenge(true, true, 0, cancellationTokenSource.Token);
		}

		[Fact]
		public void log_started() {
			Assert.True(Log.Started);
		}

		[Fact]
		public void log_completed_with_success() {
			Assert.True(Log.Completed);
			Assert.Equal(Log.Result, ScavengeResult.Success);
		}

		[Fact]
		public void scavenge_record_for_all_completed_chunks() {
			Assert.Equal(Log.Scavenged.Count, 2);
			Assert.True(Log.Scavenged[0].Scavenged);;
			Assert.Equal(Log.Scavenged[0].ChunkStart, 0);
			Assert.Equal(Log.Scavenged[0].ChunkEnd, 0);
			Assert.True(Log.Scavenged[1].Scavenged);;
			Assert.Equal(Log.Scavenged[1].ChunkStart, 1);
			Assert.Equal(Log.Scavenged[1].ChunkEnd, 1);
		}

		[Fact]
		public void merge_record_for_all_completed_merges() {
			Assert.Single(Log.Merged);
			Assert.True(Log.Merged[0].Scavenged);;
			Assert.Equal(Log.Merged[0].ChunkStart, 0);
			Assert.Equal(Log.Merged[0].ChunkEnd, 1);
		}

		[Fact]
		public void calls_scavenge_on_the_table_index() {
			Assert.Equal(FakeTableIndex.ScavengeCount, 1);
		}
	}
}
