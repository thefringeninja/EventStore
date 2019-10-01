using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.HashCollisions {
	public class
		with_three_collisioned_streams_with_different_number_of_events_third_one_deleted_each_read_index_should :
			ReadIndexTestScenario {
		private EventRecord[] _prepares1;
		private EventRecord[] _prepares2;
		private EventRecord[] _prepares3;
		private EventRecord _delete3;

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

			_delete3 = WriteDelete("EF");
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
			Assert.Null(result.Record);
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
			Assert.Equal(EventNumber.DeletedStream, ReadIndex.GetStreamLastEventNumber("EF"));
		}

		[Fact]
		public void return_minus_one_when_aked_for_last_version_for_stream_with_same_hash_as_third() {
			Assert.Equal(-1, ReadIndex.GetStreamLastEventNumber("FY"));
		}

		[Fact]
		public void not_find_first_record_for_third_stream() {
			var result = ReadIndex.ReadEvent("EF", 0);
			Assert.Equal(ReadEventResult.StreamDeleted, result.Result);
			Assert.Null(result.Record);
		}

		[Fact]
		public void not_find_last_log_record_for_third_stream() {
			var result = ReadIndex.ReadEvent("EF", 6);
			Assert.Equal(ReadEventResult.StreamDeleted, result.Result);
		}

		[Fact]
		public void not_find_record_with_version_7_in_third_stream() {
			var result = ReadIndex.ReadEvent("EF", 7);
			Assert.Equal(ReadEventResult.StreamDeleted, result.Result);
		}

		[Fact]
		public void return_not_found_for_record_version_7_for_stream_with_same_hash_as_third_stream() {
			var result = ReadIndex.ReadEvent("FY", 7);
			Assert.Equal(ReadEventResult.NoStream, result.Result);
		}

		[Fact]
		public void return_empty_range_on_from_start_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 0, 7);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_0_7_range_on_from_start_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 0, 7);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_2_3_range_on_from_start_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 2, 3);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_for_7_1_range_on_from_start_range_query_request_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsForward("EF", 7, 1);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
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
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_on_from_end_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 0, 7);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_0_1_range_on_from_end_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 0, 1);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_1_1_range_on_from_end_range_query_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 1, 1);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		[Fact]
		public void return_empty_range_for_7_1_range_on_from_end_range_query_request_for_third_stream() {
			var result = ReadIndex.ReadStreamEventsBackward("EF", 7, 1);
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
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
			Assert.Equal(ReadStreamResult.StreamDeleted, result.Result);
			Assert.Equal(0, result.Records.Length);
		}

		#endregion

		#region all

		[Fact]
		public void return_all_prepares_on_read_all_forward() {
			var events = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records.Select(r => r.Event).ToArray();
			Assert.Equal(3 + 5 + 7 + 1, events.Length);

			Assert.Equal(_prepares1[0], events[0]);
			Assert.Equal(_prepares1[1], events[1]);
			Assert.Equal(_prepares1[2], events[2]);

			Assert.Equal(_prepares2[0], events[3]);
			Assert.Equal(_prepares2[1], events[4]);
			Assert.Equal(_prepares2[2], events[5]);
			Assert.Equal(_prepares2[3], events[6]);
			Assert.Equal(_prepares2[4], events[7]);

			Assert.Equal(_prepares3[0], events[8]);
			Assert.Equal(_prepares3[1], events[9]);
			Assert.Equal(_prepares3[2], events[10]);
			Assert.Equal(_prepares3[3], events[11]);
			Assert.Equal(_prepares3[4], events[12]);
			Assert.Equal(_prepares3[5], events[13]);
			Assert.Equal(_prepares3[6], events[14]);

			Assert.Equal(_delete3, events[15]);
		}

		[Fact]
		public void return_all_prepares_on_read_all_backward() {
			var events = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records.Select(r => r.Event)
				.ToArray();
			Assert.Equal(3 + 5 + 7 + 1, events.Length);

			Assert.Equal(_prepares1[0], events[15]);
			Assert.Equal(_prepares1[1], events[14]);
			Assert.Equal(_prepares1[2], events[13]);

			Assert.Equal(_prepares2[0], events[12]);
			Assert.Equal(_prepares2[1], events[11]);
			Assert.Equal(_prepares2[2], events[10]);
			Assert.Equal(_prepares2[3], events[9]);
			Assert.Equal(_prepares2[4], events[8]);

			Assert.Equal(_prepares3[0], events[7]);
			Assert.Equal(_prepares3[1], events[6]);
			Assert.Equal(_prepares3[2], events[5]);
			Assert.Equal(_prepares3[3], events[4]);
			Assert.Equal(_prepares3[4], events[3]);
			Assert.Equal(_prepares3[5], events[2]);
			Assert.Equal(_prepares3[6], events[1]);

			Assert.Equal(_delete3, events[0]);
		}

		#endregion
	}
}
