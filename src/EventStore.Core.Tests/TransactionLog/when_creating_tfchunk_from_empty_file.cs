using System;
using System.IO;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_creating_tfchunk_from_empty_file : SpecificationWithFile {
		private TFChunk _chunk;

		public override void SetUp() {
			base.SetUp();
			_chunk = TFChunkHelper.CreateNewChunk(Filename, 1024);
		}

		public override void TearDown() {
			_chunk.Dispose();
			base.TearDown();
		}

		[Fact]
		public void the_chunk_is_not_cached() {
			Assert.False(_chunk.IsCached);
		}

		[Fact]
		public void the_file_is_created() {
			Assert.True(File.Exists(Filename));
		}

		[Fact]
		public void the_chunk_is_not_readonly() {
			Assert.False(_chunk.IsReadOnly);
		}

		[Fact]
		public void append_does_not_throw_exception() {
			_chunk.TryAppend(new CommitLogRecord(0, Guid.NewGuid(), 0, DateTime.UtcNow, 0));
		}

		[Fact]
		public void there_is_no_record_at_pos_zero() {
			var res = _chunk.TryReadAt(0);
			Assert.False(res.Success);
		}

		[Fact]
		public void there_is_no_first_record() {
			var res = _chunk.TryReadFirst();
			Assert.False(res.Success);
		}

		[Fact]
		public void there_is_no_closest_forward_record_to_pos_zero() {
			var res = _chunk.TryReadClosestForward(0);
			Assert.False(res.Success);
		}

		[Fact]
		public void there_is_no_closest_backward_record_from_end() {
			var res = _chunk.TryReadClosestForward(0);
			Assert.False(res.Success);
		}

		[Fact]
		public void there_is_no_last_record() {
			var res = _chunk.TryReadLast();
			Assert.False(res.Success);
		}
	}
}
