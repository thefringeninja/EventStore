using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using Xunit;
using System.Linq;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.HashCollisions {
	public class
		with_three_collisioned_streams_with_different_number_of_events_each_read_index_should : ReadIndexTestScenario {
		private EventRecord[] _prepares1;
		private EventRecord[] _prepares2;
		private EventRecord[] _prepares3;

		protected override void WriteTestScenario() {
			_prepares1 = new EventRecord[3];
			for (int i = 0; i < _prepares1.Length; i++) {
				_prepares1[i] = WriteSingleEvent("AB", i, "test" + i);
			}

			_prepares2 = new EventRecord[5];
			for (int i = 0; i < _prepares2.Length; i++) {
				_prepares2[i] = WriteSingleEvent("CD", i, "test" + i);
			}

			_prepares3 = new EventRecord[7];
			for (int i = 0; i < _prepares3.Length; i++) {
				_prepares3[i] = WriteSingleEvent("EF", i, "test" + i);
			}
		}

		#region first

		[Fact]
		public void return_correct_last_event_version_for_first_stream() {
			Assert.Equal(2, ReadIndex.GetStreamLastEventNumber("AB"));
		}

		[Fact]
		public void return_minus_one_when_asked_for_last_version_for_stream_with_same_hash_as_first() {
			Assert.Equal(-1, ReadIndex.GetStreamLastEventNumber("FY"));
		}

		[Fact]
		public void return_correct_first_record_for_first_stream() {
			var result = ReadIndex.ReadEvent("AB", 0);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_prepares1[0], result.Record);
		}

		[Fact]
		public void return_correct_last_log_record_for_first_stream() {
			var result = ReadIndex.ReadEvent("AB", 2);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_prepares1[2], result.Record);
		}

		[Fact]
		public void not_find_record_with_version_3_in_first_stream() {
			var result = ReadIndex.ReadEvent("AB", 3);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_not_found_for_record_version_3_for_stream_with_same_hash_as_first_stream() {
			var result = ReadIndex.ReadEvent("FY", 3);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_not_found_for_record_version_2_for_stream_with_same_hash_as_first_stream() {
			var result = ReadIndex.ReadEvent("FY", 2);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_not_found_for_record_version_0_for_stream_with_same_hash_as_first_stream() {
			var result = ReadIndex.ReadEvent("FY", 0);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_correct_range_on_from_start_range_query_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsForward("AB", 0, 3);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);

			for (int i = 0; i < _prepares1.Length; i++) {
				Assert.Equal(_prepares1[i], result.Records[i]);
			}
		}

		[Fact]
		public void return_correct_0_1_range_on_from_start_range_query_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsForward("AB", 0, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares1[0], result.Records[0]);
		}

		[Fact]
		public void return_correct_1_1_range_on_from_start_range_query_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsForward("AB", 1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares1[1], result.Records[0]);
		}

		[Fact]
		public void return_empty_range_for_3_1_range_on_from_start_range_query_request_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsForward("AB", 3, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_on_from_start_range_query_for_non_existing_stream_with_same_hash_as_first_one() {
			var result = ReadIndex.ReadStreamEventsForward("FY", 0, 3);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_1_1_range_on_from_start_range_query_for_non_existing_stream_with_same_hash_as_first_one() {
			var result = ReadIndex.ReadStreamEventsForward("FY", 1, 1);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_3_1_range_on_from_start_range_query_for_non_existing_stream_with_same_hash_as_first_one() {
			var result = ReadIndex.ReadStreamEventsForward("FY", 3, 1);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_first_stream_with_specific_version() {
			var result = ReadIndex.ReadStreamEventsBackward("AB", 2, 3);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);

			var records = result.Records.Reverse().ToArray();

			for (int i = 0; i < _prepares1.Length; i++) {
				Assert.Equal(_prepares1[i], records[i]);
			}
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_first_stream_with_from_end_version() {
			var result = ReadIndex.ReadStreamEventsBackward("AB", -1, 3);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);

			var records = result.Records.Reverse().ToArray();

			for (int i = 0; i < _prepares1.Length; i++) {
				Assert.Equal(_prepares1[i], records[i]);
			}
		}

		[Fact]
		public void return_correct_0_1_range_on_from_end_range_query_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("AB", 0, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares1[0], result.Records[0]);
		}

		[Fact]
		public void return_correct_from_end_1_range_on_from_end_range_query_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("AB", -1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares1[2], result.Records[0]);
		}

		[Fact]
		public void return_correct_1_1_range_on_from_end_range_query_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("AB", 1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares1[1], result.Records[0]);
		}

		[Fact]
		public void return_empty_range_for_3_1_range_on_from_end_range_query_request_for_first_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("AB", 3, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_on_from_end_range_query_for_non_existing_stream_with_same_hash_as_first_one() {
			var result = ReadIndex.ReadStreamEventsBackward("FY", 0, 3);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_1_1_range_on_from_end_range_query_for_non_existing_stream_with_same_hash_as_first_one() {
			var result = ReadIndex.ReadStreamEventsBackward("FY", 1, 1);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_3_1_range_on_from_end_range_query_for_non_existing_stream_with_same_hash_as_first_one() {
			var result = ReadIndex.ReadStreamEventsBackward("FY", 3, 1);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		#endregion

		#region second

		[Fact]
		public void return_correct_last_event_version_for_second_stream() {
			Assert.Equal(4, ReadIndex.GetStreamLastEventNumber("CD"));
		}

		[Fact]
		public void return_minus_one_when_aked_for_last_version_for_stream_with_same_hash_as_second() {
			Assert.Equal(-1, ReadIndex.GetStreamLastEventNumber("FY"));
		}

		[Fact]
		public void return_correct_first_record_for_second_stream() {
			var result = ReadIndex.ReadEvent("CD", 0);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_prepares2[0], result.Record);
		}

		[Fact]
		public void return_correct_last_log_record_for_second_stream() {
			var result = ReadIndex.ReadEvent("CD", 4);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_prepares2[4], result.Record);
		}

		[Fact]
		public void not_find_record_with_version_5_in_second_stream() {
			var result = ReadIndex.ReadEvent("CD", 5);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
		}

		[Fact]
		public void return_not_found_for_record_version_5_for_stream_with_same_hash_as_second_stream() {
			var result = ReadIndex.ReadEvent("FY", 5);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_not_found_for_record_version_4_for_stream_with_same_hash_as_second_stream() {
			var result = ReadIndex.ReadEvent("FY", 4);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_not_found_for_record_version_0_for_stream_with_same_hash_as_second_stream() {
			var result = ReadIndex.ReadEvent("FY", 0);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_correct_range_on_from_start_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsForward("CD", 0, 5);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(5, result.Records.Length);

			for (int i = 0; i < _prepares2.Length; i++) {
				Assert.Equal(_prepares2[i], result.Records[i]);
			}
		}

		[Fact]
		public void return_correct_0_2_range_on_from_start_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsForward("CD", 0, 2);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(2, result.Records.Length);

			Assert.Equal(_prepares2[0], result.Records[0]);
			Assert.Equal(_prepares2[1], result.Records[1]);
		}

		[Fact]
		public void return_correct_2_2_range_on_from_start_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsForward("CD", 2, 2);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(2, result.Records.Length);

			Assert.Equal(_prepares2[2], result.Records[0]);
			Assert.Equal(_prepares2[3], result.Records[1]);
		}

		[Fact]
		public void return_empty_range_for_5_1_range_on_from_start_range_query_request_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsForward("CD", 5, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_range_on_from_start_range_query_for_non_existing_stream_with_same_hash_as_second_one() {
			var result = ReadIndex.ReadStreamEventsForward("FY", 0, 5);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_5_1_range_on_from_start_range_query_for_non_existing_stream_with_same_hash_as_second_one() {
			var result = ReadIndex.ReadStreamEventsForward("FY", 5, 1);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_second_stream_with_from_end_vesion() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", -1, 5);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(5, result.Records.Length);

			var records = result.Records.Reverse().ToArray();

			for (int i = 0; i < _prepares2.Length; i++) {
				Assert.Equal(_prepares2[i], records[i]);
			}
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_second_stream_with_specific_version() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", 4, 5);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(5, result.Records.Length);

			var records = result.Records.Reverse().ToArray();

			for (int i = 0; i < _prepares2.Length; i++) {
				Assert.Equal(_prepares2[i], records[i]);
			}
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_second_stream_with_from_end_version() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", -1, 5);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(5, result.Records.Length);

			var records = result.Records.Reverse().ToArray();

			for (int i = 0; i < _prepares2.Length; i++) {
				Assert.Equal(_prepares2[i], records[i]);
			}
		}

		[Fact]
		public void return_correct_0_1_range_on_from_end_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", 0, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares2[0], result.Records[0]);
		}

		[Fact]
		public void return_correct_from_end_1_range_on_from_end_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", -1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares2[4], result.Records[0]);
		}

		[Fact]
		public void return_correct_1_1_range_on_from_end_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", 1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares2[1], result.Records[0]);
		}

		[Fact]
		public void return_correct_from_end_2_range_on_from_end_range_query_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", -1, 2);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(2, result.Records.Length);

			Assert.Equal(_prepares2[4], result.Records[0]);
			Assert.Equal(_prepares2[3], result.Records[1]);
		}

		[Fact]
		public void return_empty_range_for_5_1_range_on_from_end_range_query_request_for_second_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("CD", 5, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_on_from_end_range_query_for_non_existing_stream_with_same_hash_as_second_one() {
			var result = ReadIndex.ReadStreamEventsBackward("FY", 0, 5);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_5_1_range_on_from_end_range_query_for_non_existing_stream_with_same_hash_as_second_one() {
			var result = ReadIndex.ReadStreamEventsBackward("FY", 5, 1);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		#endregion

		#region third

		[Fact]
		public void return_correct_last_event_version_for_third_stream() {
			Assert.Equal(6, ReadIndex.GetStreamLastEventNumber("EF"));
		}

		[Fact]
		public void return_minus_one_when_aked_for_last_version_for_stream_with_same_hash_as_third() {
			Assert.Equal(-1, ReadIndex.GetStreamLastEventNumber("FY"));
		}

		[Fact]
		public void return_correct_first_record_for_third_stream() {
			var result = ReadIndex.ReadEvent("EF", 0);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_prepares3[0], result.Record);
		}

		[Fact]
		public void return_correct_last_log_record_for_third_stream() {
			var result = ReadIndex.ReadEvent("EF", 6);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_prepares3[6], result.Record);
		}

		[Fact]
		public void not_find_record_with_version_7_in_third_stream() {
			var result = ReadIndex.ReadEvent("EF", 7);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_not_found_for_record_version_7_for_stream_with_same_hash_as_third_stream() {
			var result = ReadIndex.ReadEvent("FY", 7);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void return_correct_range_on_from_start_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 0, 7);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(7, result.Records.Length);

			for (int i = 0; i < _prepares3.Length; i++) {
				Assert.Equal(_prepares3[i], result.Records[i]);
			}
		}

		[Fact]
		public void return_correct_0_7_range_on_from_start_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 0, 7);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(7, result.Records.Length);

			Assert.Equal(_prepares3[0], result.Records[0]);
			Assert.Equal(_prepares3[1], result.Records[1]);
			Assert.Equal(_prepares3[2], result.Records[2]);
			Assert.Equal(_prepares3[3], result.Records[3]);
			Assert.Equal(_prepares3[4], result.Records[4]);
			Assert.Equal(_prepares3[5], result.Records[5]);
			Assert.Equal(_prepares3[6], result.Records[6]);
		}

		[Fact]
		public void return_correct_2_3_range_on_from_start_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 2, 3);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);

			Assert.Equal(_prepares3[2], result.Records[0]);
			Assert.Equal(_prepares3[3], result.Records[1]);
			Assert.Equal(_prepares3[4], result.Records[2]);
		}

		[Fact]
		public void return_empty_range_for_7_1_range_on_from_start_range_query_request_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 7, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_on_from_start_range_query_for_non_existing_stream_with_same_hash_as_third_one() {
			var result = ReadIndex.ReadStreamEventsForward("FY", 0, 7);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_7_1_range_on_from_start_range_query_for_non_existing_stream_with_same_hash_as_third_one() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 7, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_third_stream_from_specific_version() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 6, 7);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(7, result.Records.Length);

			var records = result.Records.Reverse().ToArray();

			for (int i = 0; i < _prepares3.Length; i++) {
				Assert.Equal(_prepares3[i], records[i]);
			}
		}

		[Fact]
		public void return_correct_range_on_from_end_range_query_for_third_stream_with_from_end_version() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", -1, 7);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(7, result.Records.Length);

			var records = result.Records.Reverse().ToArray();

			for (int i = 0; i < _prepares3.Length; i++) {
				Assert.Equal(_prepares3[i], records[i]);
			}
		}

		[Fact]
		public void return_correct_0_1_range_on_from_end_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 0, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares3[0], result.Records[0]);
		}

		[Fact]
		public void return_correct_from_end_1_range_on_from_end_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", -1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares3[6], result.Records[0]);
		}

		[Fact]
		public void return_correct_1_1_range_on_from_end_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 1, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(1, result.Records.Length);

			Assert.Equal(_prepares3[1], result.Records[0]);
		}

		[Fact]
		public void return_correct_from_end_2_range_on_from_end_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", -1, 2);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(2, result.Records.Length);

			Assert.Equal(_prepares3[6], result.Records[0]);
			Assert.Equal(_prepares3[5], result.Records[1]);
		}

		[Fact]
		public void return_empty_range_for_7_1_range_on_from_end_range_query_request_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 7, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_on_from_end_range_query_for_non_existing_stream_with_same_hash_as_third_one() {
			var result = ReadIndex.ReadStreamEventsBackward("FY", 0, 7);
			Assert.Equal(ReadStreamResult.NoStream, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void
			return_empty_7_1_range_on_from_end_range_query_for_non_existing_stream_with_same_hash_as_third_one() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 7, 1);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		#endregion
	}
}
