using EventStore.Core.Data;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount {
	public class when_having_stream_with_truncatebefore_specified : ReadIndexTestScenario {
		private EventRecord _r1;
		private EventRecord _r2;
		private EventRecord _r3;
		private EventRecord _r4;
		private EventRecord _r5;
		private EventRecord _r6;

		protected override void WriteTestScenario() {
			const string metadata = @"{""$tb"":2}";

			_r1 = WriteStreamMetadata("ES", 0, metadata);
			_r2 = WriteSingleEvent("ES", 0, "bla1");
			_r3 = WriteSingleEvent("ES", 1, "bla1");
			_r4 = WriteSingleEvent("ES", 2, "bla1");
			_r5 = WriteSingleEvent("ES", 3, "bla1");
			_r6 = WriteSingleEvent("ES", 4, "bla1");
		}

		[Fact]
		public void single_event_read_doesnt_return_old_events_and_return_actual_ones() {
			var result = ReadIndex.ReadEvent("ES", 0);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);

			result = ReadIndex.ReadEvent("ES", 1);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);

			result = ReadIndex.ReadEvent("ES", 2);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r4, result.Record);

			result = ReadIndex.ReadEvent("ES", 3);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r5, result.Record);

			result = ReadIndex.ReadEvent("ES", 4);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r6, result.Record);
		}

		[Fact]
		public void forward_range_read_doesnt_return_old_records() {
			var result = ReadIndex.ReadStreamEventsForward("ES", 0, 100);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);
			Assert.Equal(_r4, result.Records[0]);
			Assert.Equal(_r5, result.Records[1]);
			Assert.Equal(_r6, result.Records[2]);
		}

		[Fact]
		public void backward_range_read_doesnt_return_expired_records() {
			var result = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(3, result.Records.Length);
			Assert.Equal(_r6, result.Records[0]);
			Assert.Equal(_r5, result.Records[1]);
			Assert.Equal(_r4, result.Records[2]);
		}

		[Fact]
		public void read_all_forward_returns_all_records_including_expired_ones() {
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
		public void read_all_backward_returns_all_records_including_expired_ones() {
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
