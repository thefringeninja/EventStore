using System;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class appending_to_implicitly_created_stream
		: IClassFixture<appending_to_implicitly_created_stream.Fixture> {
		private readonly MiniNode _node;

		public class Fixture : SpecificationWithDirectoryPerTestFixture {
			public MiniNode Node;

			public override async Task TestFixtureSetUp() {
				await base.TestFixtureSetUp();
				Node = new MiniNode(PathName);
				await Node.Start();
			}

			public override async Task TestFixtureTearDown() {
				await Node.Shutdown();
				await base.TestFixtureTearDown();
			}
		}

		public appending_to_implicitly_created_stream(Fixture fixture) {
			_node = fixture.Node;
		}

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		/*
		 * sequence - events written so stream
		 * 0em1 - event number 0 written with exp version -1 (minus 1)
		 * 1any - event number 1 written with exp version any
		 * S_0em1_1em1_E - START bucket, two events in bucket, END bucket
		*/

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);

				var tail = await writer.Append(events);

				await tail.Then(events[0], -1);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_4e4_0any_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_4e4_0any_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);

				var tail = await writer.Append(events);
				await tail.Then(events.First(), ExpectedVersion.Any);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);

				var tail = await writer.Append(events);
				await tail.Then(events.First(), 5);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length + 1);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_wev() {
			const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_wev";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);

				var first6 = await writer.Append(events);
				await Assert.ThrowsAsync<WrongExpectedVersionException>(
					() => first6.Then(events.First(), 6));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev() {
			const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);

				var first6 = await writer.Append(events);
				await Assert.ThrowsAsync<WrongExpectedVersionException>(
					() => first6.Then(events.First(), 4));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_0e0_non_idempotent() {
			const string stream = "appending_to_implicitly_created_stream_sequence_0em1_0e0_non_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);

				var tail = await writer.Append(events);
				await tail.Then(events.First(), 0);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length + 1);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_0any_idempotent() {
			const string stream = "appending_to_implicitly_created_stream_sequence_0em1_0any_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);

				var tail = await writer.Append(events);

				await tail.Then(events.First(), ExpectedVersion.Any);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_0em1_idempotent() {
			const string stream = "appending_to_implicitly_created_stream_sequence_0em1_0em1_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);

				var tail = await writer.Append(events);
				await tail.Then(events.First(), -1);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_1any_1any_idempotent() {
			const string stream = "appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_1any_1any_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 3).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new StreamWriter(store, stream, -1);


				var tailWriter = await writer.Append(events);
				tailWriter = await tailWriter.Then(events[1], ExpectedVersion.Any);
				await tailWriter.Then(events[1], ExpectedVersion.Any);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_S_0em1_1em1_E_S_0em1_E_idempotent() {
			const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0em1_E_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				await store.AppendToStreamAsync(stream, -1, events);

				await store.AppendToStreamAsync(stream, -1, events.First());

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_S_0em1_1em1_E_S_0any_E_idempotent() {
			const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0any_E_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				await store.AppendToStreamAsync(stream, -1, events);

				await store.AppendToStreamAsync(stream, ExpectedVersion.Any, new[] {events.First()});

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_S_0em1_1em1_E_S_1e0_E_idempotent() {
			const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_1e0_E_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				await store.AppendToStreamAsync(stream, -1, events);

				await store.AppendToStreamAsync(stream, 0, events[1]);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_S_0em1_1em1_E_S_1any_E_idempotent() {
			const string stream = "appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_1any_E_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				await store.AppendToStreamAsync(stream, -1, events);

				await store.AppendToStreamAsync(stream, ExpectedVersion.Any, events[1]);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail() {
			const string stream =
				"appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				await store.AppendToStreamAsync(stream, -1, events);

				await Assert.ThrowsAsync<WrongExpectedVersionException>(
					() => store.AppendToStreamAsync(stream, -1,
						events.Concat(new[] {TestEvent.NewTestEvent(Guid.NewGuid())})));
			}
		}
	}
}
