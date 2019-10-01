using System;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_appending_to_a_tfchunk_and_flushing : SpecificationWithFilePerTestFixture {
		private TFChunk _chunk;
		private readonly Guid _corrId = Guid.NewGuid();
		private readonly Guid _eventId = Guid.NewGuid();
		private RecordWriteResult _result;
		private PrepareLogRecord _record;


		public when_appending_to_a_tfchunk_and_flushing() {
			_record = new PrepareLogRecord(0, _corrId, _eventId, 0, 0, "test", 1, new DateTime(2000, 1, 1, 12, 0, 0),
				PrepareFlags.None, "Foo", new byte[12], new byte[15]);
			_chunk = TFChunkHelper.CreateNewChunk(Filename);
			_result = _chunk.TryAppend(_record);
			_chunk.Flush();
		}

		public override void Dispose() {
			_chunk.Dispose();
			base.Dispose();
		}

		[Fact]
		public void the_write_result_is_correct() {
			Assert.True(_result.Success);
			Assert.Equal(0, _result.OldPosition);
			Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), _result.NewPosition);
		}

		[Fact]
		public void the_record_is_appended() {
			Assert.True(_result.Success);
		}

		[Fact]
		public void correct_old_position_is_returned() {
			//position without header (logical position).
			Assert.Equal(0, _result.OldPosition);
		}

		[Fact]
		public void the_updated_position_is_returned() {
			//position without header (logical position).
			Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), _result.NewPosition);
		}

		[Fact]
		public void the_record_can_be_read_at_exact_position() {
			var res = _chunk.TryReadAt(0);
			Assert.True(res.Success);
			Assert.Equal(_record, res.LogRecord);
			Assert.Equal(_result.OldPosition, res.LogRecord.LogPosition);
		}

		[Fact]
		public void the_record_can_be_read_as_first_one() {
			var res = _chunk.TryReadFirst();
			Assert.True(res.Success);
			Assert.Equal(_record, res.LogRecord);
			Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
		}

		[Fact]
		public void the_record_can_be_read_as_closest_forward_to_pos_zero() {
			var res = _chunk.TryReadClosestForward(0);
			Assert.True(res.Success);
			Assert.Equal(_record, res.LogRecord);
			Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), res.NextPosition);
		}

		[Fact]
		public void the_record_can_be_read_as_closest_backward_from_end() {
			var res = _chunk.TryReadClosestBackward(_record.GetSizeWithLengthPrefixAndSuffix());
			Assert.True(res.Success);
			Assert.Equal(_record, res.LogRecord);
			Assert.Equal(0, res.NextPosition);
		}

		[Fact]
		public void the_record_can_be_read_as_last_one() {
			var res = _chunk.TryReadLast();
			Assert.True(res.Success);
			Assert.Equal(_record, res.LogRecord);
			Assert.Equal(0, res.NextPosition);
		}
	}
}
