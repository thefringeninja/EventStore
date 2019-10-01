using System.IO;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_marking_for_deletion_a_tfchunk_that_has_been_locked_and_unlocked : SpecificationWithFile {
		private TFChunk _chunk;

		public override void SetUp() {
			base.SetUp();
			_chunk = TFChunkHelper.CreateNewChunk(Filename, 1000);
			var reader = _chunk.AcquireReader();
			_chunk.MarkForDeletion();
			reader.Release();
		}

		[Fact]
		public void the_file_is_deleted() {
			Assert.False(File.Exists(Filename));
		}
	}
}
