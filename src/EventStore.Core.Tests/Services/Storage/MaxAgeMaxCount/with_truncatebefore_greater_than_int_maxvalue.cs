using System;
using EventStore.Core.Data;
using EventStore.Core.Services;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount {
	public class with_truncatebefore_greater_than_int_maxvalue : ReadIndexTestScenario {
		private EventRecord _r1;
		private EventRecord _r2;
		private EventRecord _r3;
		private EventRecord _r4;
		private EventRecord _r5;
		private EventRecord _r6;

		private const long first = (long)int.MaxValue + 1;
		private const long second = (long)int.MaxValue + 2;
		private const long third = (long)int.MaxValue + 3;
		private const long fourth = (long)int.MaxValue + 4;
		private const long fifth = (long)int.MaxValue + 5;

		protected override void WriteTestScenario() {
			var now = DateTime.UtcNow;

			string metadata = @"{""$tb"":" + third + "}";

			_r1 = WriteStreamMetadata("ES", 0, metadata, now.AddSeconds(-100));
			_r2 = WriteSingleEvent("ES", first, "bla1", now.AddSeconds(-50));
			_r3 = WriteSingleEvent("ES", second, "bla1", now.AddSeconds(-20));
			_r4 = WriteSingleEvent("ES", third, "bla1", now.AddSeconds(-11));
			_r5 = WriteSingleEvent("ES", fourth, "bla1", now.AddSeconds(-5));
			_r6 = WriteSingleEvent("ES", fifth, "bla1", now.AddSeconds(-1));
		}

		[Fact]
		public void metastream_read_returns_metaevent() {
			var result = ReadIndex.ReadEvent(SystemStreams.MetastreamOf("ES"), 0);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r1, result.Record);
		}

		[Fact]
		public void single_event_read_returns_records_after_truncate_before() {
			var result = ReadIndex.ReadEvent("ES", first);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);

			result = ReadIndex.ReadEvent("ES", second);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);

			result = ReadIndex.ReadEvent("ES", third);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r4, result.Record);

			result = ReadIndex.ReadEvent("ES", fourth);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r5, result.Record);

			result = ReadIndex.ReadEvent("ES", fifth);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r6, result.Record);
		}

		[Fact]
		public void forward_range_read_returns_records_after_truncate_before() {
			var result = ReadIndex.ReadStreamEventsForward("ES", first, 100);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);
			Assert.Equal(_r4, result.Records[0]);
			Assert.Equal(_r5, result.Records[1]);
			Assert.Equal(_r6, result.Records[2]);
		}

		[Fact]
		public void backward_range_read_returns_records_after_truncate_before() {
			var result = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);
			Assert.Equal(_r6, result.Records[0]);
			Assert.Equal(_r5, result.Records[1]);
			Assert.Equal(_r4, result.Records[2]);
		}

		[Fact]
		public void read_all_forward_returns_all_records() {
			var records = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records;
			Assert.Equal(6, records.Count);
			Assert.Equal(_r1, records[0].Event);
			Assert.Equal(_r2, records[1].Event);
			Assert.Equal(_r3, records[2].Event);
			Assert.Equal(_r4, records[3].Event);
			Assert.Equal(_r5, records[4].Event);
			Assert.Equal(_r6, records[5].Event);
		}

		[Fact]
		public void read_all_backward_returns_all_records() {
			var records = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records;
			Assert.Equal(6, records.Count);
			Assert.Equal(_r6, records[0].Event);
			Assert.Equal(_r5, records[1].Event);
			Assert.Equal(_r4, records[2].Event);
			Assert.Equal(_r3, records[3].Event);
			Assert.Equal(_r2, records[4].Event);
			Assert.Equal(_r1, records[5].Event);
		}
	}
}
