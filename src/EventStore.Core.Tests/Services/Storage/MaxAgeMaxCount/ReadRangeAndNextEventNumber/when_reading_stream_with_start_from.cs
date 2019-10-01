using EventStore.Core.Data;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount.ReadRangeAndNextEventNumber {
	public class when_reading_stream_with_truncatebefore : ReadIndexTestScenario {
		private EventRecord _event3;
		private EventRecord _event4;
		private EventRecord _event5;

		protected override void WriteTestScenario() {
			WriteStreamMetadata("ES", 0, @"{""$tb"":2}");
			WriteSingleEvent("ES", 0, "bla");
			WriteSingleEvent("ES", 1, "bla");
			_event3 = WriteSingleEvent("ES", 2, "bla");
			_event4 = WriteSingleEvent("ES", 3, "bla");
			_event5 = WriteSingleEvent("ES", 4, "bla");
		}

		[Fact]
		public void
			on_read_forward_from_start_to_expired_next_event_number_is_first_active_and_its_not_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 0, 2);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(2, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.False(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(0, records.Length);
		}

		[Fact]
		public void
			on_read_forward_from_start_to_active_next_event_number_is_last_read_event_plus_1_and_its_not_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 0, 4);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(4, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.False(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(2, records.Length);
			Assert.Equal(_event3, records[0]);
			Assert.Equal(_event4, records[1]);
		}

		[Fact]
		public void
			on_read_forward_from_expired_to_active_next_event_number_is_last_read_event_plus_1_and_its_not_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 1, 2);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(3, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.False(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(1, records.Length);
			Assert.Equal(_event3, records[0]);
		}

		[Fact]
		public void on_read_forward_from_expired_to_end_next_event_number_is_end_plus_1_and_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 1, 4);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(5, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(3, records.Length);
			Assert.Equal(_event3, records[0]);
			Assert.Equal(_event4, records[1]);
			Assert.Equal(_event5, records[2]);
		}

		[Fact]
		public void
			on_read_forward_from_expired_to_out_of_bounds_next_event_number_is_end_plus_1_and_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 1, 6);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(5, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(3, records.Length);
			Assert.Equal(_event3, records[0]);
			Assert.Equal(_event4, records[1]);
			Assert.Equal(_event5, records[2]);
		}

		[Fact]
		public void
			on_read_forward_from_out_of_bounds_to_out_of_bounds_next_event_number_is_end_plus_1_and_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 7, 2);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(5, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(0, records.Length);
		}


		[Fact]
		public void
			on_read_backward_from_end_to_active_next_event_number_is_last_read_event_minus_1_and_its_not_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 4, 2);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(2, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.False(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(2, records.Length);
			Assert.Equal(_event5, records[0]);
			Assert.Equal(_event4, records[1]);
		}

		[Fact]
		public void on_read_backward_from_end_to_maxcount_bound_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 4, 3);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(-1, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(3, records.Length);
			Assert.Equal(_event5, records[0]);
			Assert.Equal(_event4, records[1]);
			Assert.Equal(_event3, records[2]);
		}

		[Fact]
		public void on_read_backward_from_active_to_expired_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 3, 3);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(-1, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(2, records.Length);
			Assert.Equal(_event4, records[0]);
			Assert.Equal(_event3, records[1]);
		}

		[Fact]
		public void on_read_backward_from_expired_to_expired_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 1, 2);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(-1, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(0, records.Length);
		}

		[Fact]
		public void on_read_backward_from_expired_to_before_start_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 1, 5);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(-1, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(0, records.Length);
		}

		[Fact]
		public void
			on_read_backward_from_out_of_bounds_to_out_of_bounds_next_event_number_is_end_and_its_not_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 10, 3);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(4, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.False(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(0, records.Length);
		}
	}
}
