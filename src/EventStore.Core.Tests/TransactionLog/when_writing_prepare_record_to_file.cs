using System;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_writing_prepare_record_to_file : SpecificationWithDirectoryPerTestFixture, IDisposable {
		private ITransactionFileWriter _writer;
		private InMemoryCheckpoint _writerCheckpoint;
		private readonly Guid _eventId = Guid.NewGuid();
		private readonly Guid _correlationId = Guid.NewGuid();
		private PrepareLogRecord _record;
		private TFChunkDb _db;

		public when_writing_prepare_record_to_file() {
			_writerCheckpoint = new InMemoryCheckpoint();
			_db = new TFChunkDb(TFChunkHelper.CreateDbConfig(PathName, _writerCheckpoint, new InMemoryCheckpoint(),
				1024));
			_db.Open();
			_writer = new TFChunkWriter(_db);
			_writer.Open();
			_record = new PrepareLogRecord(logPosition: 0,
				eventId: _eventId,
				correlationId: _correlationId,
				transactionPosition: 0xDEAD,
				transactionOffset: 0xBEEF,
				eventStreamId: "WorldEnding",
				expectedVersion: 1234,
				timeStamp: new DateTime(2012, 12, 21),
				flags: PrepareFlags.SingleWrite,
				eventType: "type",
				data: new byte[] {1, 2, 3, 4, 5},
				metadata: new byte[] {7, 17});
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
			//TODO MAKE THIS ACTUALLY ASSERT OFF THE FILE AND READER FROM KNOWN FILE
			using (var reader = new TFChunkChaser(_db, _writerCheckpoint, _db.Config.ChaserCheckpoint, false)) {
				reader.Open();
				LogRecord r;
				Assert.True(reader.TryReadNext(out r));

				Assert.True(r is PrepareLogRecord);
				var p = (PrepareLogRecord)r;
				Assert.Equal(LogRecordType.Prepare, p.RecordType);
				Assert.Equal(0, p.LogPosition);
				Assert.Equal(0xDEAD, p.TransactionPosition);
				Assert.Equal(0xBEEF, p.TransactionOffset);
				Assert.Equal(_correlationId, p.CorrelationId);
				Assert.Equal(_eventId, p.EventId);
				Assert.Equal("WorldEnding", p.EventStreamId);
				Assert.Equal(1234, p.ExpectedVersion);
				Assert.Equal(new DateTime(2012, 12, 21), p.TimeStamp);
				Assert.Equal(PrepareFlags.SingleWrite, p.Flags);
				Assert.Equal("type", p.EventType);
				Assert.Equal(5, p.Data.Length);
				Assert.Equal(2, p.Metadata.Length);
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
