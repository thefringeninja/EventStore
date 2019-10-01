using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.Scavenge {
	public class when_stream_is_softdeleted_with_log_record_version_0 : ScavengeTestScenario {
		protected override DbResult CreateDb(TFChunkDbCreationHelper dbCreator) {
			return dbCreator.Chunk(
					Rec.Prepare(0, "$$test", metadata: new StreamMetadata(tempStream: true),
						version: LogRecordVersion.LogRecordV0),
					Rec.Commit(0, "$$test", version: LogRecordVersion.LogRecordV0),
					Rec.Prepare(1, "test", version: LogRecordVersion.LogRecordV0),
					Rec.Commit(1, "test", version: LogRecordVersion.LogRecordV0),
					Rec.Prepare(2, "test", version: LogRecordVersion.LogRecordV0),
					Rec.Commit(2, "test", version: LogRecordVersion.LogRecordV0),
					Rec.Prepare(3, "$$test",
						metadata: new StreamMetadata(truncateBefore: int.MaxValue, tempStream: true),
						version: LogRecordVersion.LogRecordV0),
					Rec.Commit(3, "$$test", version: LogRecordVersion.LogRecordV0))
				.CompleteLastChunk()
				.CreateDb();
		}

		protected override LogRecord[][] KeptRecords(DbResult dbResult) {
			return new[] {new LogRecord[0]};
		}

		[Fact]
		public void scavenging_goes_as_expected() {
		}

		[Fact]
		public void the_stream_is_absent_logically() {
			Assert.Equal(ReadEventResult.NoStream, ReadIndex.ReadEvent("test", 0).Result);
			Assert.Equal(ReadStreamResult.NoStream, ReadIndex.ReadStreamEventsForward("test", 0, 100).Result);
			Assert.Equal(ReadStreamResult.NoStream, ReadIndex.ReadStreamEventsBackward("test", -1, 100).Result);
		}

		[Fact]
		public void the_metastream_is_absent_logically() {
			Assert.Equal(ReadEventResult.NotFound, ReadIndex.ReadEvent("$$test", 0).Result);
			Assert.Equal(ReadStreamResult.Success, ReadIndex.ReadStreamEventsForward("$$test", 0, 100).Result);
			Assert.Equal(ReadStreamResult.Success, ReadIndex.ReadStreamEventsBackward("$$test", -1, 100).Result);
		}

		[Fact]
		public void the_stream_is_absent_physically() {
			var headOfTf = new TFPos(Db.Config.WriterCheckpoint.Read(), Db.Config.WriterCheckpoint.Read());
			Assert.Empty(ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 1000).Records
				.Where(x => x.Event.EventStreamId == "test"));
			Assert.Empty(ReadIndex.ReadAllEventsBackward(headOfTf, 1000).Records
				.Where(x => x.Event.EventStreamId == "test"));
		}

		[Fact]
		public void the_metastream_is_absent_physically() {
			var headOfTf = new TFPos(Db.Config.WriterCheckpoint.Read(), Db.Config.WriterCheckpoint.Read());
			Assert.Empty(ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 1000).Records
				.Where(x => x.Event.EventStreamId == "$$test"));
			Assert.Empty(ReadIndex.ReadAllEventsBackward(headOfTf, 1000).Records
				.Where(x => x.Event.EventStreamId == "$$test"));
		}
	}
}
