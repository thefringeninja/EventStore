using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class read_event_should : IClassFixture<read_event_should.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private Guid _eventId0;
		private Guid _eventId1;

		protected override async Task When() {
			_eventId0 = Guid.NewGuid();
			_eventId1 = Guid.NewGuid();

            await Connection.AppendToStreamAsync("test-stream",
					-1,
					new EventData(_eventId0, "event0", false, new byte[3], new byte[2]),
					new EventData(_eventId1, "event1", true, new byte[7], new byte[10]));
            await Connection.DeleteStreamAsync("deleted-stream", -1, hardDelete: true);
		}

		[Fact, Trait("Category", "Network")]
		public Task throw_if_stream_id_is_null() {
			return Assert.ThrowsAsync<ArgumentNullException>(() => Connection.ReadEventAsync(null, 0, resolveLinkTos: false));
		}

		[Fact, Trait("Category", "Network")]
		public Task throw_if_stream_id_is_empty() {
			return Assert.ThrowsAsync<ArgumentNullException>(() => Connection.ReadEventAsync("", 0, resolveLinkTos: false));
		}

		[Fact, Trait("Category", "Network")]
		public Task throw_if_event_number_is_less_than_minus_one() {
			return Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
				Connection.ReadEventAsync("stream", -2, resolveLinkTos: false));
		}

		[Fact, Trait("Category", "Network")]
		public async Task notify_using_status_code_if_stream_not_found() {
			var res = await Connection.ReadEventAsync("unexisting-stream", 5, false);

			Assert.Equal(EventReadStatus.NoStream, res.Status);
			Assert.Null(res.Event);
			Assert.Equal("unexisting-stream", res.Stream);
			Assert.Equal(5, res.EventNumber);
		}

		[Fact, Trait("Category", "Network")]
		public async Task return_no_stream_if_requested_last_event_in_empty_stream() {
			var res = await Connection.ReadEventAsync("some-really-empty-stream", -1, false);
			Assert.Equal(EventReadStatus.NoStream, res.Status);
		}

		[Fact, Trait("Category", "Network")]
		public async Task notify_using_status_code_if_stream_was_deleted() {
			var res = await Connection.ReadEventAsync("deleted-stream", 5, false);

			Assert.Equal(EventReadStatus.StreamDeleted, res.Status);
			Assert.Null(res.Event);
			Assert.Equal("deleted-stream", res.Stream);
			Assert.Equal(5, res.EventNumber);
		}

		[Fact, Trait("Category", "Network")]
		public async Task notify_using_status_code_if_stream_does_not_have_event() {
			var res = await Connection.ReadEventAsync("test-stream", 5, false);

			Assert.Equal(EventReadStatus.NotFound, res.Status);
			Assert.Null(res.Event);
			Assert.Equal("test-stream", res.Stream);
			Assert.Equal(5, res.EventNumber);
		}

		[Fact, Trait("Category", "Network")]
		public async Task return_existing_event() {
			var res = await Connection.ReadEventAsync("test-stream", 0, false);

			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(res.Event.Value.OriginalEvent.EventId, _eventId0);
			Assert.Equal("test-stream", res.Stream);
			Assert.Equal(0, res.EventNumber);
			Assert.NotEqual(DateTime.MinValue, res.Event.Value.OriginalEvent.Created);
			Assert.NotEqual(0, res.Event.Value.OriginalEvent.CreatedEpoch);
		}

		[Fact, Trait("Category", "Network")]
		public async Task retrieve_the_is_json_flag_properly() {
			var res = await Connection.ReadEventAsync("test-stream", 1, false);

			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(res.Event.Value.OriginalEvent.EventId, _eventId1);
			Assert.True(res.Event.Value.OriginalEvent.IsJson);
		}

		[Fact, Trait("Category", "Network")]
		public async Task return_last_event_in_stream_if_event_number_is_minus_one() {
			var res = await Connection.ReadEventAsync("test-stream", -1, false);

			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(res.Event.Value.OriginalEvent.EventId, _eventId1);
			Assert.Equal("test-stream", res.Stream);
			Assert.Equal(-1, res.EventNumber);
			Assert.NotEqual(DateTime.MinValue, res.Event.Value.OriginalEvent.Created);
			Assert.NotEqual(0, res.Event.Value.OriginalEvent.CreatedEpoch);
		}
	}
}
