using System;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount {
	public class with_too_big_truncatebefore : ReadIndexTestScenario {
		private EventRecord _r1;
		private EventRecord _r2;
		private EventRecord _r3;
		private EventRecord _r4;
		private EventRecord _r5;
		private EventRecord _r6;

		protected override void WriteTestScenario() {
			var now = DateTime.UtcNow;

			const string metadata = @"{""$tb"":9223372036854775808}"; //long.maxValue + 1

			_r1 = WriteStreamMetadata("ES", 0, metadata, now.AddSeconds(-100));
			_r2 = WriteSingleEvent("ES", 0, "bla1", now.AddSeconds(-50));
			_r3 = WriteSingleEvent("ES", 1, "bla1", now.AddSeconds(-20));
			_r4 = WriteSingleEvent("ES", 2, "bla1", now.AddSeconds(-11));
			_r5 = WriteSingleEvent("ES", 3, "bla1", now.AddSeconds(-5));
			_r6 = WriteSingleEvent("ES", 4, "bla1", now.AddSeconds(-1));
		}

		[Fact]
		public void single_event_read_returns_all_records() {
			var result = ReadIndex.ReadEvent("ES", 0);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r2, result.Record);

			result = ReadIndex.ReadEvent("ES", 1);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r3, result.Record);

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
		public void forward_range_read_returns_all_records() {
			var result = ReadIndex.ReadStreamEventsForward("ES", 0, 100);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(5, result.Records.Length);
			Assert.Equal(_r2, result.Records[0]);
			Assert.Equal(_r3, result.Records[1]);
			Assert.Equal(_r4, result.Records[2]);
			Assert.Equal(_r5, result.Records[3]);
			Assert.Equal(_r6, result.Records[4]);
		}

		[Fact]
		public void backward_range_read_returns_all_records() {
			var result = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(5, result.Records.Length);
			Assert.Equal(_r2, result.Records[4]);
			Assert.Equal(_r3, result.Records[3]);
			Assert.Equal(_r4, result.Records[2]);
			Assert.Equal(_r5, result.Records[1]);
			Assert.Equal(_r6, result.Records[0]);
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
