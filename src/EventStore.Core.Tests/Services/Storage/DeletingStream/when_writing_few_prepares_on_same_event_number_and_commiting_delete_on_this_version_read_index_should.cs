using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.DeletingStream {
	public class
		when_writing_few_prepares_with_same_event_number_and_commiting_delete_on_this_version_read_index_should :
			ReadIndexTestScenario {
		private EventRecord _deleteTombstone;

		protected override void WriteTestScenario() {
			long pos;

			var prepare1 = LogRecord.SingleWrite(WriterCheckpoint.ReadNonFlushed(), // prepare1
				Guid.NewGuid(),
				Guid.NewGuid(),
				"ES",
				-1,
				"some-type",
				LogRecord.NoData,
				null,
				DateTime.UtcNow);
			Assert.True(Writer.Write(prepare1, out pos));

			var prepare2 = LogRecord.SingleWrite(WriterCheckpoint.ReadNonFlushed(), // prepare2
				Guid.NewGuid(),
				Guid.NewGuid(),
				"ES",
				-1,
				"some-type",
				LogRecord.NoData,
				null,
				DateTime.UtcNow);
			Assert.True(Writer.Write(prepare2, out pos));


			var deletePrepare = LogRecord.DeleteTombstone(WriterCheckpoint.ReadNonFlushed(), // delete prepare
				Guid.NewGuid(), Guid.NewGuid(), "ES", -1);
			_deleteTombstone = new EventRecord(EventNumber.DeletedStream, deletePrepare);
			Assert.True(Writer.Write(deletePrepare, out pos));

			var prepare3 = LogRecord.SingleWrite(WriterCheckpoint.ReadNonFlushed(), // prepare3
				Guid.NewGuid(),
				Guid.NewGuid(),
				"ES",
				-1,
				"some-type",
				LogRecord.NoData,
				null,
				DateTime.UtcNow);
			Assert.True(Writer.Write(prepare3, out pos));

			var commit = LogRecord.Commit(WriterCheckpoint.ReadNonFlushed(), // committing delete
				deletePrepare.CorrelationId,
				deletePrepare.LogPosition,
				EventNumber.DeletedStream);
			Assert.True(Writer.Write(commit, out pos));
		}

		[Fact]
		public void indicate_that_stream_is_deleted() {
			Assert.True(ReadIndex.IsStreamDeleted("ES"));
		}

		[Fact]
		public void indicate_that_nonexisting_stream_with_same_hash_is_not_deleted() {
			Assert.False(ReadIndex.IsStreamDeleted("ZZ"));
		}

		[Fact]
		public void indicate_that_nonexisting_stream_with_different_hash_is_not_deleted() {
			Assert.False(ReadIndex.IsStreamDeleted("XXX"));
		}

		[Fact]
		public void read_single_events_with_number_0_should_return_stream_deleted() {
			var result = ReadIndex.ReadEvent("ES", 0);
			Assert.Equal(ReadEventResult.StreamDeleted, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void read_single_events_with_number_1_should_return_stream_deleted() {
			var result = ReadIndex.ReadEvent("ES", 1);
			Assert.Equal(ReadEventResult.StreamDeleted, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void read_stream_events_forward_should_return_stream_deleted() {
			var result = ReadIndex.ReadStreamEventsForward("ES", 0, 100);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void read_stream_events_backward_should_return_stream_deleted() {
			var result = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void read_all_forward_should_return_all_stream_records_except_uncommited() {
			var events = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records.Select(r => r.Event).ToArray();
			Assert.Equal(1, events.Length);
			Assert.Equal(_deleteTombstone, events[0]);
		}

		[Fact]
		public void read_all_backward_should_return_all_stream_records_except_uncommited() {
			var events = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records.Select(r => r.Event)
				.ToArray();
			Assert.Equal(1, events.Length);
			Assert.Equal(_deleteTombstone, events[0]);
		}
	}
}
