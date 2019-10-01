using System;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_reading_from_a_cached_tfchunk : SpecificationWithFilePerTestFixture {
		private TFChunk _chunk;
		private readonly Guid _corrId = Guid.NewGuid();
		private readonly Guid _eventId = Guid.NewGuid();
		private TFChunk _cachedChunk;
		private PrepareLogRecord _record;
		private RecordWriteResult _result;

		public when_reading_from_a_cached_tfchunk() {
			_record = new PrepareLogRecord(0, _corrId, _eventId, 0, 0, "test", 1, new DateTime(2000, 1, 1, 12, 0, 0),
				PrepareFlags.None, "Foo", new byte[12], new byte[15]);
			_chunk = TFChunkHelper.CreateNewChunk(Filename);
			_result = _chunk.TryAppend(_record);
			_chunk.Flush();
			_chunk.Complete();
			_cachedChunk = TFChunk.FromCompletedFile(Filename, verifyHash: true, unbufferedRead: false,
				initialReaderCount: 5, reduceFileCachePressure: false);
			_cachedChunk.CacheInMemory();
		}

		public override void Dispose() {
			_chunk.Dispose();
			_cachedChunk.Dispose();
			base.Dispose();
		}

		[Fact]
		public void the_write_result_is_correct() {
			Assert.True(_result.Success);
			Assert.Equal(0, _result.OldPosition);
			Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), _result.NewPosition);
		}

		[Fact]
		public void the_chunk_is_cached() {
			Assert.True(_cachedChunk.IsCached);
		}

		[Fact]
		public void the_record_can_be_read_at_exact_position() {
			var res = _cachedChunk.TryReadAt(0);
			Assert.True(res.Success);
			Assert.Equal(_record, res.LogRecord);
			Assert.Equal(_result.OldPosition, res.LogRecord.LogPosition);
		}

		[Fact]
		public void the_record_can_be_read_as_first_record() {
			var res = _cachedChunk.TryReadFirst();
			Assert.True(res.Success);
			Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
			Assert.Equal(_record, res.LogRecord);
			Assert.Equal(_result.OldPosition, res.LogRecord.LogPosition);
		}

		[Fact]
		public void the_record_can_be_read_as_closest_forward_to_zero_pos() {
			var res = _cachedChunk.TryReadClosestForward(0);
			Assert.True(res.Success);
			Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
			Assert.Equal(_record, res.LogRecord);
			Assert.Equal(_result.OldPosition, res.LogRecord.LogPosition);
		}

		[Fact]
		public void the_record_can_be_read_as_closest_backward_from_end() {
			var res = _cachedChunk.TryReadClosestBackward(_record.GetSizeWithLengthPrefixAndSuffix());
			Assert.True(res.Success);
			Assert.Equal(0, res.NextPosition);
			Assert.Equal(_record, res.LogRecord);
		}

		[Fact]
		public void the_record_can_be_read_as_last() {
			var res = _cachedChunk.TryReadLast();
			Assert.True(res.Success);
			Assert.Equal(0, res.NextPosition);
			Assert.Equal(_record, res.LogRecord);
		}
	}
}
