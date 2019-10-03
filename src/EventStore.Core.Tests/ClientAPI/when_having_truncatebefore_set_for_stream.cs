﻿using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class when_having_truncatebefore_set_for_stream : IClassFixture<when_having_truncatebefore_set_for_stream.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private EventData[] _testEvents;

		protected override Task When() {
			_testEvents = Enumerable.Range(0, 5).Select(x => TestEvent.NewTestEvent(data: x.ToString())).ToArray();
			return Task.CompletedTask;
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task read_event_respects_truncatebefore() {
			const string stream = "read_event_respects_truncatebefore";
            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadEventAsync(stream, 1, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 2, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[2].EventId, res.Event.Value.OriginalEvent.EventId);
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task read_stream_forward_respects_truncatebefore() {
			const string stream = "read_stream_forward_respects_truncatebefore";
            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task read_stream_backward_respects_truncatebefore() {
			const string stream = "read_stream_backward_respects_truncatebefore";
            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task after_setting_less_strict_truncatebefore_read_event_reads_more_events() {
			const string stream = "after_setting_less_strict_truncatebefore_read_event_reads_more_events";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadEventAsync(stream, 1, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 2, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[2].EventId, res.Event.Value.OriginalEvent.EventId);

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(1));

			res = await Connection.ReadEventAsync(stream, 0, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 1, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[1].EventId, res.Event.Value.OriginalEvent.EventId);
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task after_setting_more_strict_truncatebefore_read_event_reads_less_events() {
			const string stream = "after_setting_more_strict_truncatebefore_read_event_reads_less_events";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadEventAsync(stream, 1, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 2, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[2].EventId, res.Event.Value.OriginalEvent.EventId);

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(3));

			res = await Connection.ReadEventAsync(stream, 2, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 3, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[3].EventId, res.Event.Value.OriginalEvent.EventId);
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task less_strict_max_count_doesnt_change_anything_for_event_read() {
			const string stream = "less_strict_max_count_doesnt_change_anything_for_event_read";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadEventAsync(stream, 1, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 2, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[2].EventId, res.Event.Value.OriginalEvent.EventId);

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(2).SetMaxCount(4));

			res = await Connection.ReadEventAsync(stream, 1, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 2, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[2].EventId, res.Event.Value.OriginalEvent.EventId);
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task more_strict_max_count_gives_less_events_for_event_read() {
			const string stream = "more_strict_max_count_gives_less_events_for_event_read";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadEventAsync(stream, 1, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 2, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[2].EventId, res.Event.Value.OriginalEvent.EventId);

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(2).SetMaxCount(2));

			res = await Connection.ReadEventAsync(stream, 2, false);
			Assert.Equal(EventReadStatus.NotFound, res.Status);

			res = await Connection.ReadEventAsync(stream, 3, false);
			Assert.Equal(EventReadStatus.Success, res.Status);
			Assert.Equal(_testEvents[3].EventId, res.Event.Value.OriginalEvent.EventId);
		}


		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task after_setting_less_strict_truncatebefore_read_stream_forward_reads_more_events() {
			const string stream = "after_setting_less_strict_truncatebefore_read_stream_forward_reads_more_events";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(1));

			res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(4, res.Events.Length);
			Assert.Equal(_testEvents.Skip(1).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task after_setting_more_strict_truncatebefore_read_stream_forward_reads_less_events() {
			const string stream = "after_setting_more_strict_truncatebefore_read_stream_forward_reads_less_events";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(3));

			res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(2, res.Events.Length);
			Assert.Equal(_testEvents.Skip(3).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task less_strict_max_count_doesnt_change_anything_for_stream_forward_read() {
			const string stream = "less_strict_max_count_doesnt_change_anything_for_stream_forward_read";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(2).SetMaxCount(4));

			res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task more_strict_max_count_gives_less_events_for_stream_forward_read() {
			const string stream = "more_strict_max_count_gives_less_events_for_stream_forward_read";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(2).SetMaxCount(2));

			res = await Connection.ReadStreamEventsForwardAsync(stream, 0, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(2, res.Events.Length);
			Assert.Equal(_testEvents.Skip(3).Select(x => x.EventId).ToArray(),
				res.Events.Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task after_setting_less_strict_truncatebefore_read_stream_backward_reads_more_events() {
			const string stream = "after_setting_less_strict_truncatebefore_read_stream_backward_reads_more_events";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(1));

			res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(4, res.Events.Length);
			Assert.Equal(_testEvents.Skip(1).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task after_setting_more_strict_truncatebefore_read_stream_backward_reads_less_events() {
			const string stream = "after_setting_more_strict_truncatebefore_read_stream_backward_reads_less_events";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(3));

			res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(2, res.Events.Length);
			Assert.Equal(_testEvents.Skip(3).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task less_strict_max_count_doesnt_change_anything_for_stream_backward_read() {
			const string stream = "less_strict_max_count_doesnt_change_anything_for_stream_backward_read";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(2).SetMaxCount(4));

			res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task more_strict_max_count_gives_less_events_for_stream_backward_read() {
			const string stream = "more_strict_max_count_gives_less_events_for_stream_backward_read";

            await Connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, _testEvents);

            await Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				StreamMetadata.Build().SetTruncateBefore(2));

			var res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(3, res.Events.Length);
			Assert.Equal(_testEvents.Skip(2).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());

            await Connection.SetStreamMetadataAsync(stream, 0, StreamMetadata.Build().SetTruncateBefore(2).SetMaxCount(2));

			res = await Connection.ReadStreamEventsBackwardAsync(stream, -1, 100, false);
			Assert.Equal(SliceReadStatus.Success, res.Status);
			Assert.Equal(2, res.Events.Length);
			Assert.Equal(_testEvents.Skip(3).Select(x => x.EventId).ToArray(),
				res.Events.Reverse().Select(x => x.Event.EventId).ToArray());
		}
	}
}
