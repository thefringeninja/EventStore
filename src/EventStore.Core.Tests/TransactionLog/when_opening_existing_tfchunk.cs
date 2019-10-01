using System;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_opening_existing_tfchunk : SpecificationWithFilePerTestFixture {
		private TFChunk _chunk;
		private TFChunk _testChunk;

		public when_opening_existing_tfchunk() {
			_chunk = TFChunkHelper.CreateNewChunk(Filename);
			_chunk.Complete();
			_testChunk = TFChunk.FromCompletedFile(Filename, true, false, 5, reduceFileCachePressure: false);
		}

		public override void Dispose() {
			_chunk.Dispose();
			_testChunk.Dispose();
			base.Dispose();
		}

		[Fact]
		public void the_chunk_is_not_cached() {
			Assert.False(_testChunk.IsCached);
		}

		[Fact]
		public void the_chunk_is_readonly() {
			Assert.True(_testChunk.IsReadOnly);
		}

		[Fact]
		public void append_throws_invalid_operation_exception() {
			Assert.Throws<InvalidOperationException>(() =>
				_testChunk.TryAppend(new CommitLogRecord(0, Guid.NewGuid(), 0, DateTime.UtcNow, 0)));
		}

		[Fact]
		public void flush_throws_invalid_operation_exception() {
			Assert.Throws<InvalidOperationException>(() => _testChunk.Flush());
		}
	}
}
