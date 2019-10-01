using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.Transactions {
	public class when_having_multievent_sequential_write_request_read_index_should : ReadIndexTestScenario {
		private EventRecord _p1;
		private EventRecord _p2;
		private EventRecord _p3;

		protected override void WriteTestScenario() {
			_p1 = WriteTransactionBegin("ES", ExpectedVersion.NoStream, 0, "test1");
			_p2 = WriteTransactionEvent(_p1.CorrelationId, _p1.LogPosition, 1, _p1.EventStreamId, 1, "test2",
				PrepareFlags.Data);
			_p3 = WriteTransactionEvent(_p1.CorrelationId, _p1.LogPosition, 2, _p1.EventStreamId, 2, "test3",
				PrepareFlags.TransactionEnd | PrepareFlags.Data);

			WriteCommit(_p1.CorrelationId, _p1.LogPosition, _p1.EventStreamId, _p1.EventNumber);
		}

		[Fact]
		public void return_correct_last_event_version_for_stream() {
			Assert.Equal(2, ReadIndex.GetStreamLastEventNumber("ES"));
		}

		[Fact]
		public void return_correct_first_record_for_stream() {
			var result = ReadIndex.ReadEvent("ES", 0);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_p1, result.Record);
		}

		[Fact]
		public void return_correct_second_record_for_stream() {
			var result = ReadIndex.ReadEvent("ES", 1);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_p2, result.Record);
		}

		[Fact]
		public void return_correct_third_record_for_stream() {
			var result = ReadIndex.ReadEvent("ES", 2);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_p3, result.Record);
		}

		[Fact]
		public void not_find_record_with_nonexistent_version() {
			var result = ReadIndex.ReadEvent("ES", 3);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_correct_range_on_from_start_range_query_for_stream() {
			var result = ReadIndex.ReadStreamEventsForward("ES", 0, 3);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);
			Assert.Equal(_p1, result.Records[0]);
			Assert.Equal(_p2, result.Records[1]);
			Assert.Equal(_p3, result.Records[2]);
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_stream_with_specific_event_version() {
			var result = ReadIndex.ReadStreamEventsBackward("ES", 2, 3);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);
			Assert.Equal(_p3, result.Records[0]);
			Assert.Equal(_p2, result.Records[1]);
			Assert.Equal(_p1, result.Records[2]);
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_stream_with_from_end_version() {
			var result = ReadIndex.ReadStreamEventsBackward("ES", -1, 3);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);
			Assert.Equal(_p3, result.Records[0]);
			Assert.Equal(_p2, result.Records[1]);
			Assert.Equal(_p1, result.Records[2]);
		}

		[Fact]
		public void read_all_events_forward_returns_all_events_in_correct_order() {
			var records = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 10).Records;

			Assert.Equal(3, records.Count);
			Assert.Equal(_p1, records[0].Event);
			Assert.Equal(_p2, records[1].Event);
			Assert.Equal(_p3, records[2].Event);
		}

		[Fact]
		public void read_all_events_backward_returns_all_events_in_correct_order() {
			var records = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records;

			Assert.Equal(3, records.Count);
			Assert.Equal(_p1, records[2].Event);
			Assert.Equal(_p2, records[1].Event);
			Assert.Equal(_p3, records[0].Event);
		}
	}
}
