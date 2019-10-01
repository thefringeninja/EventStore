using System;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.MaxAgeMaxCount.AfterScavenge {
	public class when_having_stream_with_maxage_specified : ReadIndexTestScenario {
		private EventRecord _r1;
		private EventRecord _r5;
		private EventRecord _r6;

		protected override void WriteTestScenario() {
			var now = DateTime.UtcNow;

			var metadata = string.Format(@"{{""$maxAge"":{0}}}", (int)TimeSpan.FromMinutes(10).TotalSeconds);

			_r1 = WriteStreamMetadata("ES", 0, metadata);
			WriteSingleEvent("ES", 0, "bla1", now.AddMinutes(-50));
			WriteSingleEvent("ES", 1, "bla1", now.AddMinutes(-20));
			WriteSingleEvent("ES", 2, "bla1", now.AddMinutes(-11));
			_r5 = WriteSingleEvent("ES", 3, "bla1", now.AddMinutes(-5));
			_r6 = WriteSingleEvent("ES", 4, "bla1", now.AddMinutes(-1));

			Scavenge(completeLast: true, mergeChunks: false);
		}

		[Fact]
		public void single_event_read_doesnt_return_expired_events_and_returns_all_actual_ones() {
			var result = ReadIndex.ReadEvent("ES", 0);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);

			result = ReadIndex.ReadEvent("ES", 1);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);

			result = ReadIndex.ReadEvent("ES", 2);
			Assert.Equal(ReadEventResult.NotFound, result.Result);
			Assert.Null(result.Record);

			result = ReadIndex.ReadEvent("ES", 3);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r5, result.Record);

			result = ReadIndex.ReadEvent("ES", 4);
			Assert.Equal(ReadEventResult.Success, result.Result);
			Assert.Equal(_r6, result.Record);
		}

		[Fact]
		public void forward_range_read_doesnt_return_expired_records() {
			var result = ReadIndex.ReadStreamEventsForward("ES", 0, 100);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(2, result.Records.Length);
			Assert.Equal(_r5, result.Records[0]);
			Assert.Equal(_r6, result.Records[1]);
		}

		[Fact]
		public void backward_range_read_doesnt_return_expired_records() {
			var result = ReadIndex.ReadStreamEventsBackward("ES", -1, 100);
			Assert.Equal(ReadStreamResult.Success, result.Result);
			Assert.Equal(2, result.Records.Length);
			Assert.Equal(_r6, result.Records[0]);
			Assert.Equal(_r5, result.Records[1]);
		}

		[Fact]
		public void read_all_forward_doesnt_return_expired_records() {
			var records = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records;
			Assert.Equal(3, records.Count);
			Assert.Equal(_r1, records[0].Event);
			Assert.Equal(_r5, records[1].Event);
			Assert.Equal(_r6, records[2].Event);
		}

		[Fact]
		public void read_all_backward_doesnt_return_expired_records() {
			var records = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records;
			Assert.Equal(3, records.Count);
			Assert.Equal(_r6, records[0].Event);
			Assert.Equal(_r5, records[1].Event);
			Assert.Equal(_r1, records[2].Event);
		}
	}
}
