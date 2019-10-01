using System.Collections.Generic;
using System;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Xunit;
using EventStore.Core.Data;

namespace EventStore.Core.Tests.ClientAPI.ExpectedVersion64Bit {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class read_stream_with_event_numbers_greater_than_2_billion : MiniNodeWithExistingRecords {
		private const string StreamName = "read_stream_with_event_numbers_greater_than_2_billion";
		private const long intMaxValue = (long)int.MaxValue;

		private EventRecord _r1, _r2, _r3, _r4, _r5;

		public override void WriteTestScenario() {
			_r1 = WriteSingleEvent(StreamName, intMaxValue + 1, new string('.', 3000));
			_r2 = WriteSingleEvent(StreamName, intMaxValue + 2, new string('.', 3000));
			_r3 = WriteSingleEvent(StreamName, intMaxValue + 3, new string('.', 3000));
			_r4 = WriteSingleEvent(StreamName, intMaxValue + 4, new string('.', 3000));
			_r5 = WriteSingleEvent(StreamName, intMaxValue + 5, new string('.', 3000));
		}

		public override async Task Given() {
			_store = BuildConnection(Node);
            await _store.ConnectAsync();
            await _store.SetStreamMetadataAsync(StreamName, EventStore.ClientAPI.ExpectedVersion.Any,
				EventStore.ClientAPI.StreamMetadata.Create(truncateBefore: intMaxValue + 1));
		}

		[Fact]
		public async Task read_forward_from_zero() {
			var result = await _store.ReadStreamEventsForwardAsync(StreamName, 0, 100, false);
			Assert.Equal(0, result.Events.Length);
			Assert.Equal(intMaxValue + 1, result.NextEventNumber);
		}

		[Fact]
		public async Task should_be_able_to_read_stream_forward() {
			var result = await _store.ReadStreamEventsForwardAsync(StreamName, intMaxValue, 100, false);
			Assert.Equal(5, result.Events.Count());
			Assert.Equal(_r1.EventId, result.Events[0].Event.EventId);
			Assert.Equal(_r2.EventId, result.Events[1].Event.EventId);
			Assert.Equal(_r3.EventId, result.Events[2].Event.EventId);
			Assert.Equal(_r4.EventId, result.Events[3].Event.EventId);
			Assert.Equal(_r5.EventId, result.Events[4].Event.EventId);
		}

		[Fact]
		public async Task should_be_able_to_read_stream_backward() {
			var result = await _store.ReadStreamEventsBackwardAsync(StreamName, intMaxValue + 6, 100, false);
			Assert.Equal(5, result.Events.Count());
			Assert.Equal(_r5.EventId, result.Events[0].Event.EventId);
			Assert.Equal(_r4.EventId, result.Events[1].Event.EventId);
			Assert.Equal(_r3.EventId, result.Events[2].Event.EventId);
			Assert.Equal(_r2.EventId, result.Events[3].Event.EventId);
			Assert.Equal(_r1.EventId, result.Events[4].Event.EventId);
		}

		[Fact]
		public async Task should_be_able_to_read_each_event() {
			var record = await _store.ReadEventAsync(StreamName, intMaxValue + 1, false);
			Assert.Equal(EventReadStatus.Success, record.Status);
			Assert.Equal(_r1.EventId, record.Event.Value.Event.EventId);

			record = await _store.ReadEventAsync(StreamName, intMaxValue + 2, false);
			Assert.Equal(EventReadStatus.Success, record.Status);
			Assert.Equal(_r2.EventId, record.Event.Value.Event.EventId);

			record = await _store.ReadEventAsync(StreamName, intMaxValue + 3, false);
			Assert.Equal(EventReadStatus.Success, record.Status);
			Assert.Equal(_r3.EventId, record.Event.Value.Event.EventId);

			record = await _store.ReadEventAsync(StreamName, intMaxValue + 4, false);
			Assert.Equal(EventReadStatus.Success, record.Status);
			Assert.Equal(_r4.EventId, record.Event.Value.Event.EventId);

			record = await _store.ReadEventAsync(StreamName, intMaxValue + 5, false);
			Assert.Equal(EventReadStatus.Success, record.Status);
			Assert.Equal(_r5.EventId, record.Event.Value.Event.EventId);
		}

		[Fact]
		public async Task should_be_able_to_read_all_forward() {
			var result = await _store.ReadAllEventsForwardAsync(Position.Start, 100, false, DefaultData.AdminCredentials)
;
			Assert.True(result.Events.Count() > 5);

			var records = result.Events.Where(x => x.OriginalStreamId == StreamName).ToList();
			Assert.Equal(_r1.EventId, records[0].Event.EventId);
			Assert.Equal(_r2.EventId, records[1].Event.EventId);
			Assert.Equal(_r3.EventId, records[2].Event.EventId);
			Assert.Equal(_r4.EventId, records[3].Event.EventId);
			Assert.Equal(_r5.EventId, records[4].Event.EventId);
		}

		[Fact]
		public async Task should_be_able_to_read_all_backward() {
			var result = await _store.ReadAllEventsBackwardAsync(Position.End, 100, false, DefaultData.AdminCredentials)
;
			Assert.True(result.Events.Count() > 5);

			var records = result.Events.Where(x => x.OriginalStreamId == StreamName).ToList();
			Assert.Equal(_r5.EventId, records[0].Event.EventId);
			Assert.Equal(_r4.EventId, records[1].Event.EventId);
			Assert.Equal(_r3.EventId, records[2].Event.EventId);
			Assert.Equal(_r2.EventId, records[3].Event.EventId);
			Assert.Equal(_r1.EventId, records[4].Event.EventId);
		}
	}
}
