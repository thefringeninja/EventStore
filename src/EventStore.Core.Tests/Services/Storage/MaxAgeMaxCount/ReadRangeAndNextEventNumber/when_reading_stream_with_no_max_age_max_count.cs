using EventStore.Core.Data;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount.ReadRangeAndNextEventNumber {
	public class when_reading_stream_with_no_max_age_max_count : ReadIndexTestScenario {
		private EventRecord _event0;
		private EventRecord _event1;
		private EventRecord _event2;
		private EventRecord _event3;
		private EventRecord _event4;

		protected override void WriteTestScenario() {
			_event0 = WriteSingleEvent("ES", 0, "bla");
			_event1 = WriteSingleEvent("ES", 1, "bla");
			_event2 = WriteSingleEvent("ES", 2, "bla");
			_event3 = WriteSingleEvent("ES", 3, "bla");
			_event4 = WriteSingleEvent("ES", 4, "bla");
		}

		[Fact]
		public void
			on_read_forward_from_start_to_middle_next_event_number_is_middle_plus_1_and_its_not_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 0, 3);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(3, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.False(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(3, records.Length);
			Assert.Equal(_event0, records[0]);
			Assert.Equal(_event1, records[1]);
			Assert.Equal(_event2, records[2]);
		}

		[Fact]
		public void on_read_forward_from_the_middle_to_end_next_event_number_is_end_plus_1_and_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 1, 4);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(5, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(4, records.Length);
			Assert.Equal(_event1, records[0]);
			Assert.Equal(_event2, records[1]);
			Assert.Equal(_event3, records[2]);
			Assert.Equal(_event4, records[3]);
		}

		[Fact]
		public void
			on_read_forward_from_the_middle_to_out_of_bounds_next_event_number_is_end_plus_1_and_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 1, 5);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(5, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(4, records.Length);
			Assert.Equal(_event1, records[0]);
			Assert.Equal(_event2, records[1]);
			Assert.Equal(_event3, records[2]);
			Assert.Equal(_event4, records[3]);
		}

		[Fact]
		public void
			on_read_forward_from_the_out_of_bounds_to_out_of_bounds_next_event_number_is_end_plus_1_and_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsForward("ES", 6, 2);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(5, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(0, records.Length);
		}


		[Fact]
		public void
			on_read_backward_from_the_end_to_middle_next_event_number_is_middle_minus_1_and_its_not_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 4, 3);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(1, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.False(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(3, records.Length);
			Assert.Equal(_event4, records[0]);
			Assert.Equal(_event3, records[1]);
			Assert.Equal(_event2, records[2]);
		}

		[Fact]
		public void on_read_backward_from_middle_to_start_next_event_number_is_minus_1_and_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 2, 3);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(-1, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(3, records.Length);
			Assert.Equal(_event2, records[0]);
			Assert.Equal(_event1, records[1]);
			Assert.Equal(_event0, records[2]);
		}

		[Fact]
		public void on_read_backward_from_middle_to_before_start_next_event_number_is_minus_1_and_its_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 2, 5);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(-1, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.True(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(3, records.Length);
			Assert.Equal(_event2, records[0]);
			Assert.Equal(_event1, records[1]);
			Assert.Equal(_event0, records[2]);
		}

		[Fact]
		public void
			on_read_backward_from_out_of_bounds_to_middle_next_event_number_is_middle_minus_1_and_its_not_end_of_stream() {
			var res = ReadIndex.ReadStreamEventsBackward("ES", 6, 5);
			Assert.Equal(ReadStreamResult.Success, res.Result);
			Assert.Equal(1, res.NextEventNumber);
			Assert.Equal(4, res.LastEventNumber);
			Assert.False(res.IsEndOfStream);

			var records = res.Records;
			Assert.Equal(3, records.Length);
			Assert.Equal(_event4, records[0]);
			Assert.Equal(_event3, records[1]);
			Assert.Equal(_event2, records[2]);
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
