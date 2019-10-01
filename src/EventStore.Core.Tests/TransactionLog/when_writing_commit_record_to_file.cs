using System;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_writing_commit_record_to_file : SpecificationWithDirectoryPerTestFixture, IDisposable {
		private ITransactionFileWriter _writer;
		private InMemoryCheckpoint _writerCheckpoint;
		private readonly Guid _eventId = Guid.NewGuid();
		private CommitLogRecord _record;
		private TFChunkDb _db;

		public when_writing_commit_record_to_file() {
			_writerCheckpoint = new InMemoryCheckpoint();
			_db = new TFChunkDb(TFChunkHelper.CreateDbConfig(PathName, _writerCheckpoint, new InMemoryCheckpoint(),
				1024));
			_db.Open();
			_writer = new TFChunkWriter(_db);
			_writer.Open();
			_record = new CommitLogRecord(logPosition: 0,
				correlationId: _eventId,
				transactionPosition: 4321,
				timeStamp: new DateTime(2012, 12, 21),
				firstEventNumber: 10);
			long newPos;
			_writer.Write(_record, out newPos);
			_writer.Flush();
		}

		public void Dispose() {
			_writer.Close();
			_db.Close();
		}

		[Fact]
		public void the_data_is_written() {
			using (var reader = new TFChunkChaser(_db, _writerCheckpoint, _db.Config.ChaserCheckpoint, false)) {
				reader.Open();
				LogRecord r;
				Assert.True(reader.TryReadNext(out r));

				Assert.True(r is CommitLogRecord);
				var c = (CommitLogRecord)r;
				Assert.Equal(LogRecordType.Commit, c.RecordType);
				Assert.Equal(0, c.LogPosition);
				Assert.Equal(c.CorrelationId, _eventId);
				Assert.Equal(4321, c.TransactionPosition);
				Assert.Equal(c.TimeStamp, new DateTime(2012, 12, 21));
			}
		}

		[Fact]
		public void the_checksum_is_updated() {
			Assert.Equal(_record.GetSizeWithLengthPrefixAndSuffix(), _writerCheckpoint.Read());
		}

		[Fact]
		public void trying_to_read_past_writer_checksum_returns_false() {
			var reader = new TFChunkReader(_db, _writerCheckpoint);
			Assert.False(reader.TryReadAt(_writerCheckpoint.Read()).Success);
		}
	}
}
