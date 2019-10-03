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
	public class
		appending_to_implicitly_created_stream_using_transaction : IClassFixture<
			appending_to_implicitly_created_stream_using_transaction.Fixture> {
		private readonly MiniNode _node;

		public class Fixture : SpecificationWithDirectoryPerTestFixture {
			public MiniNode _node;

			public override async Task TestFixtureSetUp() {
				await base.TestFixtureSetUp();
				_node = new MiniNode(PathName);
				await _node.Start();
			}

			public override async Task TestFixtureTearDown() {
				await _node.Shutdown();
				await base.TestFixtureTearDown();
			}
		}

		virtual protected IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		public appending_to_implicitly_created_stream_using_transaction(Fixture fixture) {
			_node = fixture._node;
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
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(5,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				Assert.Equal(0,
					(await (await (await writer.StartTransaction(-1)).Write(events.First())).Commit())
					.NextExpectedVersion);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_5e4_0any_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0any_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(5,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				Assert.Equal(0,
					(await (await (await writer.StartTransaction(ExpectedVersion.Any)).Write(events.First())).Commit())
					.NextExpectedVersion);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(5,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				Assert.Equal(6,
					(await (await (await writer.StartTransaction(5)).Write(events.First())).Commit())
					.NextExpectedVersion);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length + 1);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_wev() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_wev";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(5,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				await Assert.ThrowsAsync<WrongExpectedVersionException>(async () =>
					await (await (await writer.StartTransaction(6)).Write(events.First())).Commit());
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 6).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(5,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				await Assert.ThrowsAsync<WrongExpectedVersionException>(
					async () => await (await (await writer.StartTransaction(4)).Write(events.First())).Commit());
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_0e0_non_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_0e0_non_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(0,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				Assert.Equal(1,
					(await (await (await writer.StartTransaction(0)).Write(events.First())).Commit())
					.NextExpectedVersion);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length + 1);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_0any_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_0any_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(0,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				Assert.Equal(0,
					(await (await (await writer.StartTransaction(ExpectedVersion.Any)).Write(events.First())).Commit())
					.NextExpectedVersion);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_0em1_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_0em1_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 1).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(0,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				Assert.Equal(0,
					(await (await (await writer.StartTransaction(-1)).Write(events.First())).Commit())
					.NextExpectedVersion);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_0em1_1e0_2e1_1any_1any_idempotent() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_0em1_1e0_2e1_1any_1any_idempotent";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 3).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(2,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				Assert.Equal(1,
					(await (await (await (await writer.StartTransaction(ExpectedVersion.Any)).Write(events[1])).Write(
						events[1])).Commit()).NextExpectedVersion);

				var total = await EventsStream.Count(store, stream);
				Assert.Equal(total, events.Length);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail() {
			const string stream =
				"appending_to_implicitly_created_stream_using_transaction_sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail";
			using (var store = BuildConnection(_node)) {
				await store.ConnectAsync();

				var events = Enumerable.Range(0, 2).Select(x => TestEvent.NewTestEvent(Guid.NewGuid())).ToArray();
				var writer = new TransactionalWriter(store, stream);

				Assert.Equal(1,
					(await (await (await writer.StartTransaction(-1)).Write(events)).Commit()).NextExpectedVersion);
				await Assert.ThrowsAsync<WrongExpectedVersionException>(async () =>
					await (await (await writer.StartTransaction(-1)).Write(events
						.Concat(new[] {TestEvent.NewTestEvent(Guid.NewGuid())}).ToArray())).Commit());
			}
		}
	}
}
