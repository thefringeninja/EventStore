using System;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Chunks;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Scavenging {
	public class when_scavenge_throws_exception_processing_chunk : ScavengeLifeCycleScenario {
		protected override Task When() {
			var cancellationTokenSource = new CancellationTokenSource();

			Log.ChunkScavenged += (sender, args) => {
				if (args.Scavenged)
					throw new Exception("Expected exception.");
			};

			return TfChunkScavenger.Scavenge(true, true, 0, cancellationTokenSource.Token);
		}

		[Fact]
		public void no_exception_is_thrown_to_caller() {
			Assert.True(Log.Completed);
			Assert.Equal(Log.Result, ScavengeResult.Failed);
		}

		[Fact]
		public void doesnt_call_scavenge_on_the_table_index() {
			Assert.Equal(FakeTableIndex.ScavengeCount, 0);
		}
	}
}
