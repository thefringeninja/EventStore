using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.Metastreams {
	public class
		when_having_multiple_metaevents_in_metastream_and_read_index_is_set_to_keep_last_2 : SimpleDbTestScenario {
		public when_having_multiple_metaevents_in_metastream_and_read_index_is_set_to_keep_last_2()
			: base(metastreamMaxCount: 2) {
		}

		protected override DbResult CreateDb(TFChunkDbCreationHelper dbCreator) {
			return dbCreator.Chunk(
					Rec.Prepare(0, "$$test", "0", metadata: new StreamMetadata(10, null, null, null, null)),
					Rec.Prepare(0, "$$test", "1", metadata: new StreamMetadata(9, null, null, null, null)),
					Rec.Prepare(0, "$$test", "2", metadata: new StreamMetadata(8, null, null, null, null)),
					Rec.Prepare(0, "$$test", "3", metadata: new StreamMetadata(7, null, null, null, null)),
					Rec.Prepare(0, "$$test", "4", metadata: new StreamMetadata(6, null, null, null, null)),
					Rec.Commit(0, "$$test"))
				.CreateDb();
		}

		[Fact]
		public void last_event_read_returns_correct_event() {
			var res = ReadIndex.ReadEvent("$$test", -1);
			Assert.Equal(ReadEventResult.Success, res.Result);
			Assert.Equal("4", res.Record.EventType);
		}

		[Fact]
		public void last_event_stream_number_is_correct() {
			Assert.Equal(4, ReadIndex.GetStreamLastEventNumber("$$test"));
		}

		[Fact]
		public void single_event_read_returns_last_two_events() {
			Assert.Equal(ReadEventResult.NotFound, ReadIndex.ReadEvent("$$test", 0).Result);
			Assert.Equal(ReadEventResult.NotFound, ReadIndex.ReadEvent("$$test", 1).Result);
			Assert.Equal(ReadEventResult.NotFound, ReadIndex.ReadEvent("$$test", 2).Result);

			var res = ReadIndex.ReadEvent("$$test", 3);
			Assert.Equal(ReadEventResult.Success, res.Result);
			Assert.Equal("3", res.Record.EventType);

			res = ReadIndex.ReadEvent("$$test", 4);
			Assert.Equal(ReadEventResult.Success, res.Result);
			Assert.Equal("4", res.Record.EventType);
		}

		[Fact]
		public void stream_read_forward_returns_last_two_events() {
			var res = ReadIndex.ReadStreamEventsForward("$$test", 0, 100);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(2, res.Records.Length);
			Assert.Equal("3", res.Records[0].EventType);
			Assert.Equal("4", res.Records[1].EventType);
		}

		[Fact]
		public void stream_read_backward_returns_last_two_events() {
			var res = ReadIndex.ReadStreamEventsBackward("$$test", -1, 100);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(2, res.Records.Length);
			Assert.Equal("4", res.Records[0].EventType);
			Assert.Equal("3", res.Records[1].EventType);
		}

		[Fact]
		public void metastream_metadata_is_correct() {
			var metadata = ReadIndex.GetStreamMetadata("$$test");
			Assert.Equal(2, metadata.MaxCount);
			Assert.Null(metadata.MaxAge);
		}

		[Fact]
		public void original_stream_metadata_is_taken_from_last_metaevent() {
			var metadata = ReadIndex.GetStreamMetadata("test");
			Assert.Equal(6, metadata.MaxCount);
			Assert.Null(metadata.MaxAge);
		}
	}
}
