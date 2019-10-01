using System.IO;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_destroying_a_tfchunk : SpecificationWithFile {
		private TFChunk _chunk;

		public override void SetUp() {
			base.SetUp();
			_chunk = TFChunkHelper.CreateNewChunk(Filename, 1000);
			_chunk.MarkForDeletion();
		}

		[Fact]
		public void the_file_is_deleted() {
			Assert.False(File.Exists(Filename));
		}
	}
}
