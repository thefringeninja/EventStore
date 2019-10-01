using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.HashCollisions {
	public class with_two_collisioned_streams_one_event_each_read_index_should : ReadIndexTestScenario {
		private EventRecord _prepare1;
		private EventRecord _prepare2;

		protected override void WriteTestScenario() {
			_prepare1 = WriteSingleEvent("AB", 0, "test1");
			_prepare2 = WriteSingleEvent("CD", 0, "test2");
		}

		[Fact]
		public void return_correct_last_event_version_for_first_stream() {
			Assert.Equal(0, ReadIndex.GetStreamLastEventNumber("AB"));
		}

		[Fact]
		public void return_correct_log_record_for_first_stream() {
			var result = ReadIndex.ReadEvent("AB", 0);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_prepare1, result.Record);
		}

		[Fact]
		public void return_correct_range_on_from_start_range_query_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsForward("AB", 0, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);
			Assert.Equal(_prepare1, result.Records[0]);
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("AB", 0, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);
			Assert.Equal(_prepare1, result.Records[0]);
		}

		[Fact]
		public void return_empty_range_on_from_start_range_query_for_invalid_arguments_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsForward("AB", 1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_on_from_end_range_query_for_invalid_arguments_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("AB", 1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_correct_last_event_version_for_second_stream() {
			Assert.Equal(0, ReadIndex.GetStreamLastEventNumber("CD"));
		}

		[Fact]
		public void return_correct_log_record_for_second_stream() {
			var result = ReadIndex.ReadEvent("CD", 0);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_prepare2, result.Record);
		}

		[Fact]
		public void return_correct_range_on_from_start_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsForward("CD", 0, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);
			Assert.Equal(_prepare2, result.Records[0]);
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", 0, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);
			Assert.Equal(_prepare2, result.Records[0]);
		}

		[Fact]
		public void return_correct_last_event_version_for_nonexistent_stream_with_same_hash() {
			Assert.Equal(-1, ReadIndex.GetStreamLastEventNumber("EF"));
		}

		[Fact]
		public void not_find_log_record_for_nonexistent_stream_with_same_hash() {
			var result = ReadIndex.ReadEvent("EF", 0);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void not_return_range_for_non_existing_stream_with_same_hash() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 0, 1);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}
	}
}
