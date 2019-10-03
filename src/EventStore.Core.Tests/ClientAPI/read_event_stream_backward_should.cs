using System;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class read_event_stream_backward_should : IClassFixture<read_event_stream_backward_should.Fixture> { public class Fixture : SpecificationWithDirectoryPerTestFixture {
		private MiniNode _node;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName);
			await _node.Start();
		}

		public override async Task TestFixtureTearDown() {
			await _node.Shutdown();
			await base.TestFixtureTearDown();
		}

		virtual protected IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task throw_if_count_le_zero() {
			const string stream = "read_event_stream_backward_should_throw_if_count_le_zero";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();
				await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
					store.ReadStreamEventsBackwardAsync(stream, 0, 0, resolveLinkTos: false));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task throw_if_start_less_than_minus_one() {
			const string stream = "read_event_stream_backward_should_throw_if_start_less_than_minus_one";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();
				await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
					store.ReadStreamEventsBackwardAsync(stream, -2, 100, resolveLinkTos: false));
			}
		}


		[Fact]
		[Trait("Category", "Network")]
		public async Task notify_using_status_code_if_stream_not_found() {
			const string stream = "read_event_stream_backward_should_notify_using_status_code_if_stream_not_found";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();
				var read = await store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1,
					resolveLinkTos: false);

				Assert.Equal(SliceReadStatus.StreamNotFound, read.Status);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task notify_using_status_code_if_stream_was_deleted() {
			const string stream = "read_event_stream_backward_should_notify_using_status_code_if_stream_was_deleted";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();
				await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				var read = await store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1,
					resolveLinkTos: false);

				Assert.Equal(SliceReadStatus.StreamDeleted, read.Status);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task return_no_events_when_called_on_empty_stream() {
			const string stream = "read_event_stream_backward_should_return_single_event_when_called_on_empty_stream";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var read = await store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1,
					resolveLinkTos: false);

				Assert.Empty(read.Events);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task return_partial_slice_if_no_enough_events_in_stream() {
			const string stream =
				"read_event_stream_backward_should_return_partial_slice_if_no_enough_events_in_stream";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent((x + 1).ToString()))
					.ToArray();
				var write10 = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, testEvents);

				var read = await store.ReadStreamEventsBackwardAsync(stream, 1, 5, resolveLinkTos: false);

				Assert.Equal(2, read.Events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task return_events_reversed_compared_to_written() {
			const string stream = "read_event_stream_backward_should_return_events_reversed_compared_to_written";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent((x + 1).ToString()))
					.ToArray();
				var write10 = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, testEvents);

				var read = await store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, testEvents.Length,
					resolveLinkTos: false);

				Assert.True(EventDataComparer.Equal(testEvents.Reverse().ToArray(),
					read.Events.Select(x => x.Event).ToArray()));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task be_able_to_read_single_event_from_arbitrary_position() {
			const string stream =
				"read_event_stream_backward_should_be_able_to_read_single_event_from_arbitrary_position";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
				var write10 = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, testEvents);

				var read = await store.ReadStreamEventsBackwardAsync(stream, 7, 1, resolveLinkTos: false);

				Assert.True(EventDataComparer.Equal(testEvents[7], read.Events.Single().Event));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task be_able_to_read_first_event() {
			const string stream = "read_event_stream_backward_should_be_able_to_read_first_event";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent((x + 1).ToString()))
					.ToArray();
				var write10 = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, testEvents);

				var read = await store.ReadStreamEventsBackwardAsync(stream, StreamPosition.Start, 1,
					resolveLinkTos: false);

				Assert.Single(read.Events);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task be_able_to_read_last_event() {
			const string stream = "read_event_stream_backward_should_be_able_to_read_last_event";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
				var write10 = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, testEvents);

				var read = await store.ReadStreamEventsBackwardAsync(stream, StreamPosition.End, 1,
					resolveLinkTos: false);

				Assert.True(EventDataComparer.Equal(testEvents.Last(), read.Events.Single().Event));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task be_able_to_read_slice_from_arbitrary_position() {
			const string stream = "read_event_stream_backward_should_be_able_to_read_slice_from_arbitrary_position";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var testEvents = Enumerable.Range(0, 10).Select(x => TestEvent.NewTestEvent(x.ToString())).ToArray();
				var write10 = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, testEvents);

				var read = await store.ReadStreamEventsBackwardAsync(stream, 3, 2, resolveLinkTos: false);

				Assert.True(EventDataComparer.Equal(testEvents.Skip(2).Take(2).Reverse().ToArray(),
					read.Events.Select(x => x.Event).ToArray()));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task throw_when_got_int_max_value_as_maxcount() {
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				await Assert.ThrowsAsync<ArgumentException>(() =>
					store.ReadStreamEventsBackwardAsync("foo", StreamPosition.Start, int.MaxValue,
						resolveLinkTos: false));
			}
		}
	}
}
