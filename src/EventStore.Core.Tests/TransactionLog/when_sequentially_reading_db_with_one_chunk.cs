using System;
using System.Threading.Tasks;
using EventStore.Core.Data;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_sequentially_reading_db_with_one_chunk : SpecificationWithDirectoryPerTestFixture {
		private const int RecordsCount = 3;

		private TFChunkDb _db;
		private LogRecord[] _records;
		private RecordWriteResult[] _results;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			_db = new TFChunkDb(TFChunkHelper.CreateDbConfig(PathName, 0, chunkSize: 4096));
			_db.Open();

			var chunk = _db.Manager.GetChunk(0);

			_records = new LogRecord[RecordsCount];
			_results = new RecordWriteResult[RecordsCount];

			for (int i = 0; i < _records.Length; ++i) {
				_records[i] = LogRecord.SingleWrite(i == 0 ? 0 : _results[i - 1].NewPosition,
					Guid.NewGuid(), Guid.NewGuid(), "es1", ExpectedVersion.Any, "et1",
					new byte[] {0, 1, 2}, new byte[] {5, 7});
				_results[i] = chunk.TryAppend(_records[i]);
			}

			chunk.Flush();
			_db.Config.WriterCheckpoint.Write(_results[RecordsCount - 1].NewPosition);
			_db.Config.WriterCheckpoint.Flush();
		}

		public override Task TestFixtureTearDown() {
			_db.Dispose();

			return base.TestFixtureTearDown();
		}

		[Fact]
		public void all_records_were_written() {
			var pos = 0;
			for (int i = 0; i < RecordsCount; ++i) {
				Assert.True(_results[i].Success);
				Assert.Equal(pos, _results[i].OldPosition);

				pos += _records[i].GetSizeWithLengthPrefixAndSuffix();
				Assert.Equal(pos, _results[i].NewPosition);
			}
		}

		[Fact]
		public void all_records_could_be_read_with_forward_pass() {
			var seqReader = new TFChunkReader(_db, _db.Config.WriterCheckpoint, 0);

			SeqReadResult res;
			int count = 0;
			while ((res = seqReader.TryReadNext()).Success) {
				var rec = _records[count];
				Assert.Equal(rec, res.LogRecord);
				Assert.Equal(rec.LogPosition, res.RecordPrePosition);
				Assert.Equal(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

				++count;
			}

			Assert.Equal(RecordsCount, count);
		}

		[Fact]
		public void only_the_last_record_is_marked_eof() {
			var seqReader = new TFChunkReader(_db, _db.Config.WriterCheckpoint, 0);

			SeqReadResult res;
			int count = 0;
			while ((res = seqReader.TryReadNext()).Success) {
				++count;
				Assert.Equal(count == RecordsCount, res.Eof);
			}

			Assert.Equal(RecordsCount, count);
		}

		[Fact]
		public void all_records_could_be_read_with_backward_pass() {
			var seqReader = new TFChunkReader(_db, _db.Config.WriterCheckpoint, _db.Config.WriterCheckpoint.Read());

			SeqReadResult res;
			int count = 0;
			while ((res = seqReader.TryReadPrev()).Success) {
				var rec = _records[RecordsCount - count - 1];
				Assert.Equal(rec, res.LogRecord);
				Assert.Equal(rec.LogPosition, res.RecordPrePosition);
				Assert.Equal(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

				++count;
			}

			Assert.Equal(RecordsCount, count);
		}

		[Fact]
		public void all_records_could_be_read_doing_forward_backward_pass() {
			var seqReader = new TFChunkReader(_db, _db.Config.WriterCheckpoint, 0);

			SeqReadResult res;
			int count1 = 0;
			while ((res = seqReader.TryReadNext()).Success) {
				var rec = _records[count1];
				Assert.Equal(rec, res.LogRecord);
				Assert.Equal(rec.LogPosition, res.RecordPrePosition);
				Assert.Equal(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

				++count1;
			}

			Assert.Equal(RecordsCount, count1);

			int count2 = 0;
			while ((res = seqReader.TryReadPrev()).Success) {
				var rec = _records[RecordsCount - count2 - 1];
				Assert.Equal(rec, res.LogRecord);
				Assert.Equal(rec.LogPosition, res.RecordPrePosition);
				Assert.Equal(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

				++count2;
			}

			Assert.Equal(RecordsCount, count2);
		}

		[Fact]
		public void records_can_be_read_forward_starting_from_any_position() {
			for (int i = 0; i < RecordsCount; ++i) {
				var seqReader = new TFChunkReader(_db, _db.Config.WriterCheckpoint, _records[i].LogPosition);

				SeqReadResult res;
				int count = 0;
				while ((res = seqReader.TryReadNext()).Success) {
					var rec = _records[i + count];
					Assert.Equal(rec, res.LogRecord);
					Assert.Equal(rec.LogPosition, res.RecordPrePosition);
					Assert.Equal(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

					++count;
				}

				Assert.Equal(RecordsCount - i, count);
			}
		}

		[Fact]
		public void records_can_be_read_backward_starting_from_any_position() {
			for (int i = 0; i < RecordsCount; ++i) {
				var seqReader = new TFChunkReader(_db, _db.Config.WriterCheckpoint, _records[i].LogPosition);

				SeqReadResult res;
				int count = 0;
				while ((res = seqReader.TryReadPrev()).Success) {
					var rec = _records[i - count - 1];
					Assert.Equal(rec, res.LogRecord);
					Assert.Equal(rec.LogPosition, res.RecordPrePosition);
					Assert.Equal(rec.LogPosition + rec.GetSizeWithLengthPrefixAndSuffix(), res.RecordPostPosition);

					++count;
				}

				Assert.Equal(i, count);
			}
		}
	}
}
