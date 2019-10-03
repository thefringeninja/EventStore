using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Services;
using EventStore.Core.Tests.ClientAPI.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class read_all_events_backward_should : IClassFixture<read_all_events_backward_should.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private EventData[] _testEvents;

		protected override async Task When() {
            await Connection.SetStreamMetadataAsync("$all", -1,
					StreamMetadata.Build().SetReadRole(SystemRoles.All),
					DefaultData.AdminCredentials);

			_testEvents = Enumerable.Range(0, 20).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
            await Connection.AppendToStreamAsync("stream", ExpectedVersion.NoStream, _testEvents);
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task return_empty_slice_if_asked_to_read_from_start() {
			var read = await Connection.ReadAllEventsBackwardAsync(Position.Start, 1, false);
			Assert.True(read.IsEndOfStream);
			Assert.Empty(read.Events);
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task return_partial_slice_if_not_enough_events() {
			var read = await Connection.ReadAllEventsBackwardAsync(Position.End, 30, false);
			Assert.True(read.Events.Length < 30);
			Assert.True(EventDataComparer.Equal(_testEvents.Reverse().ToArray(),
				read.Events.Take(_testEvents.Length).Select(x => x.Event).ToArray()));
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task return_events_in_reversed_order_compared_to_written() {
			var read = await Connection.ReadAllEventsBackwardAsync(Position.End, _testEvents.Length, false);
			Assert.True(EventDataComparer.Equal(_testEvents.Reverse().ToArray(),
				read.Events.Select(x => x.Event).ToArray()));
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task be_able_to_read_all_one_by_one_until_end_of_stream() {
			var all = new List<RecordedEvent>();
			var position = Position.End;
			AllEventsSlice slice;

			while (!(slice = await Connection.ReadAllEventsBackwardAsync(position, 1, false)).IsEndOfStream) {
				all.Add(slice.Events.Single().Event);
				position = slice.NextPosition;
			}

			Assert.True(
				EventDataComparer.Equal(_testEvents.Reverse().ToArray(), all.Take(_testEvents.Length).ToArray()));
		}

		[Fact, Trait("Category", "LongRunning")]
		public async Task be_able_to_read_events_slice_at_time() {
			var all = new List<RecordedEvent>();
			var position = Position.End;
			AllEventsSlice slice;

			while (!(slice = await Connection.ReadAllEventsBackwardAsync(position, 5, false)).IsEndOfStream) {
				all.AddRange(slice.Events.Select(x => x.Event));
				position = slice.NextPosition;
			}

			Assert.True(
				EventDataComparer.Equal(_testEvents.Reverse().ToArray(), all.Take(_testEvents.Length).ToArray()));
		}

		[Fact]
		[Trait("Category", "Network")]
		public Task throw_when_got_int_max_value_as_maxcount() {
			return Assert.ThrowsAsync<ArgumentException>(
				() => Connection.ReadAllEventsBackwardAsync(Position.Start, int.MaxValue, resolveLinkTos: false));
		}
	}
}
