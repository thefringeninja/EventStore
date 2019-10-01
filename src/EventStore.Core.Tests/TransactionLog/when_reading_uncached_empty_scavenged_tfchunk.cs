using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_reading_uncached_empty_scavenged_tfchunk : SpecificationWithFilePerTestFixture {
		private TFChunk _chunk;


		public when_reading_uncached_empty_scavenged_tfchunk() {
			_chunk = TFChunkHelper.CreateNewChunk(Filename, isScavenged: true);
			_chunk.CompleteScavenge(new PosMap[0]);
		}

		public override void Dispose() {
			_chunk.Dispose();
			base.Dispose();
		}

		[Fact]
		public void no_record_at_exact_position_can_be_read() {
			Assert.False(_chunk.TryReadAt(0).Success);
		}

		[Fact]
		public void no_record_can_be_read_as_first_record() {
			Assert.False(_chunk.TryReadFirst().Success);
		}

		[Fact]
		public void no_record_can_be_read_as_closest_forward_record() {
			Assert.False(_chunk.TryReadClosestForward(0).Success);
		}

		[Fact]
		public void no_record_can_be_read_as_closest_backward_record() {
			Assert.False(_chunk.TryReadClosestBackward(0).Success);
		}

		[Fact]
		public void no_record_can_be_read_as_last_record() {
			Assert.False(_chunk.TryReadLast().Success);
		}
	}
}
