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
	public class append_to_stream : IClassFixture<append_to_stream.Fixture> {
		private readonly TcpType _tcpType = TcpType.Normal;
		private readonly MiniNode _node;

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.To(node, _tcpType);
		}

		public append_to_stream(Fixture fixture) {
			_node = fixture.Node;
		}

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

		[Fact, Trait("Category", "Network")]
		public async Task should_allow_appending_zero_events_to_stream_with_no_problems() {
			const string stream1 = "should_allow_appending_zero_events_to_stream_with_no_problems1";
			const string stream2 = "should_allow_appending_zero_events_to_stream_with_no_problems2";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

                Assert.Equal(-1,
	                (await store.AppendToStreamAsync(stream1, ExpectedVersion.Any)).NextExpectedVersion);
                Assert.Equal(-1,
	                (await store.AppendToStreamAsync(stream1, ExpectedVersion.NoStream)).NextExpectedVersion);
				Assert.Equal(-1, (await store.AppendToStreamAsync(stream1, ExpectedVersion.Any)).NextExpectedVersion);
				Assert.Equal(-1,
					(await store.AppendToStreamAsync(stream1, ExpectedVersion.NoStream)).NextExpectedVersion);

				var read1 = await store.ReadStreamEventsForwardAsync(stream1, 0, 2, resolveLinkTos: false);
				Assert.Empty(read1.Events);

				Assert.Equal(-1, (await store.AppendToStreamAsync(stream2, ExpectedVersion.NoStream)).NextExpectedVersion);
				Assert.Equal(-1, (await store.AppendToStreamAsync(stream2, ExpectedVersion.Any)).NextExpectedVersion);
				Assert.Equal(-1, (await store.AppendToStreamAsync(stream2, ExpectedVersion.NoStream)).NextExpectedVersion);
				Assert.Equal(-1, (await store.AppendToStreamAsync(stream2, ExpectedVersion.Any)).NextExpectedVersion);

				var read2 = await store.ReadStreamEventsForwardAsync(stream2, 0, 2, resolveLinkTos: false);
				Assert.Empty(read2.Events);
			}
		}

		[Fact, Trait("Category", "Network")]
		public async Task should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist() {
			const string stream = "should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				Assert.Equal(0, (await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent()))
					.NextExpectedVersion);

				var read = await store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
				Assert.Single(read.Events);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist() {
			const string stream = "should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				Assert.Equal(0, (await store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent())).NextExpectedVersion);

				var read = await store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
				Assert.Single(read.Events);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task multiple_idempotent_writes() {
			const string stream = "multiple_idempotent_writes";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = new[] {
					TestEvent.NewTestEvent(), TestEvent.NewTestEvent(), TestEvent.NewTestEvent(),
					TestEvent.NewTestEvent()
				};
				Assert.Equal(3,
					(await store.AppendToStreamAsync(stream, ExpectedVersion.Any, events)).NextExpectedVersion);
				Assert.Equal(3,
					(await store.AppendToStreamAsync(stream, ExpectedVersion.Any, events)).NextExpectedVersion);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task multiple_idempotent_writes_with_same_id_bug_case() {
			const string stream = "multiple_idempotent_writes_with_same_id_bug_case";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var x = TestEvent.NewTestEvent();
				var events = new[] {x, x, x, x, x, x};
				Assert.Equal(5, (await store.AppendToStreamAsync(stream, ExpectedVersion.Any, events)).NextExpectedVersion);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task in_wtf_multiple_case_of_multiple_writes_expected_version_any_per_all_same_id() {
			const string stream = "in_wtf_multiple_case_of_multiple_writes_expected_version_any_per_all_same_id";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var x = TestEvent.NewTestEvent();
				var events = new[] {x, x, x, x, x, x};
				Assert.Equal(5, (await store.AppendToStreamAsync(stream, ExpectedVersion.Any, events)).NextExpectedVersion);
				var f = await store.AppendToStreamAsync(stream, ExpectedVersion.Any, events);
				Assert.Equal(0, f.NextExpectedVersion);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task in_slightly_reasonable_multiple_case_of_multiple_writes_with_expected_version_per_all_same_id() {
			const string stream =
				"in_slightly_reasonable_multiple_case_of_multiple_writes_with_expected_version_per_all_same_id";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var x = TestEvent.NewTestEvent();
				var events = new[] {x, x, x, x, x, x};
				Assert.Equal(5,
					(await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, events)).NextExpectedVersion);
				var f = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, events);
				Assert.Equal(5, f.NextExpectedVersion);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_fail_writing_with_correct_exp_ver_to_deleted_stream() {
			const string stream = "should_fail_writing_with_correct_exp_ver_to_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				await Assert.ThrowsAsync<StreamDeletedException>(
					() => store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent()));
				
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_return_log_position_when_writing() {
			const string stream = "should_return_log_position_when_writing";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var result = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent());
				Assert.True(0 < result.LogPosition.PreparePosition);
				Assert.True(0 < result.LogPosition.CommitPosition);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_fail_writing_with_any_exp_ver_to_deleted_stream() {
			const string stream = "should_fail_writing_with_any_exp_ver_to_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

                await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				await Assert.ThrowsAsync<StreamDeletedException>(
					() => store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent()));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_fail_writing_with_invalid_exp_ver_to_deleted_stream() {
			const string stream = "should_fail_writing_with_invalid_exp_ver_to_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				await Assert.ThrowsAsync<StreamDeletedException>(() => store.AppendToStreamAsync(stream, 5, TestEvent.NewTestEvent()));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_append_with_correct_exp_ver_to_existing_stream() {
			const string stream = "should_append_with_correct_exp_ver_to_existing_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
                await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent());

				await store.AppendToStreamAsync(stream, 0, new[] {TestEvent.NewTestEvent()});
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_append_with_any_exp_ver_to_existing_stream() {
			const string stream = "should_append_with_any_exp_ver_to_existing_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				Assert.Equal(0, (await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent())).NextExpectedVersion);
				Assert.Equal(1, (await store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent())).NextExpectedVersion);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_fail_appending_with_wrong_exp_ver_to_existing_stream() {
			const string stream = "should_fail_appending_with_wrong_exp_ver_to_existing_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

                var wev = await Assert.ThrowsAsync<WrongExpectedVersionException>(() =>
	                store.AppendToStreamAsync(stream, 1, new[] {TestEvent.NewTestEvent()}));
				Assert.Equal(1, wev.ExpectedVersion);
				Assert.Equal(ExpectedVersion.NoStream, wev.ActualVersion);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_append_with_stream_exists_exp_ver_to_existing_stream() {
			const string stream = "should_append_with_stream_exists_exp_ver_to_existing_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
                await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent());

				await store.AppendToStreamAsync(stream, ExpectedVersion.StreamExists, TestEvent.NewTestEvent());
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_append_with_stream_exists_exp_ver_to_stream_with_multiple_events() {
			const string stream = "should_append_with_stream_exists_exp_ver_to_stream_with_multiple_events";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				for (var i = 0; i < 5; i++) {
                    await store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent());
				}

				await store.AppendToStreamAsync(stream, ExpectedVersion.StreamExists, TestEvent.NewTestEvent());
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_append_with_stream_exists_exp_ver_if_metadata_stream_exists() {
			const string stream = "should_append_with_stream_exists_exp_ver_if_metadata_stream_exists";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
                await store.SetStreamMetadataAsync(stream, ExpectedVersion.Any,
					new StreamMetadata(10, null, null, null, null));

				await store.AppendToStreamAsync(stream, ExpectedVersion.StreamExists, TestEvent.NewTestEvent());
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_fail_appending_with_stream_exists_exp_ver_and_stream_does_not_exist() {
			const string stream = "should_fail_appending_with_stream_exists_exp_ver_and_stream_does_not_exist";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

                var wev = await Assert.ThrowsAsync<WrongExpectedVersionException>(
	                () => store.AppendToStreamAsync(stream, ExpectedVersion.StreamExists, TestEvent.NewTestEvent()));
				Assert.Equal(ExpectedVersion.StreamExists, wev.ExpectedVersion);
				Assert.Equal(ExpectedVersion.NoStream, wev.ActualVersion);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_fail_appending_with_stream_exists_exp_ver_to_hard_deleted_stream() {
			const string stream = "should_fail_appending_with_stream_exists_exp_ver_to_hard_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				await Assert.ThrowsAsync<StreamDeletedException>(
					() => store.AppendToStreamAsync(stream, ExpectedVersion.StreamExists, TestEvent.NewTestEvent()));
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task should_fail_appending_with_stream_exists_exp_ver_to_soft_deleted_stream() {
			const string stream = "should_fail_appending_with_stream_exists_exp_ver_to_soft_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: false);

				await Assert.ThrowsAsync<StreamDeletedException>(
					() => store.AppendToStreamAsync(stream, ExpectedVersion.StreamExists, TestEvent.NewTestEvent()));
			}
		}

		[Fact, Trait("Category", "Network")]
		public async Task can_append_multiple_events_at_once() {
			const string stream = "can_append_multiple_events_at_once";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = Enumerable.Range(0, 100).Select(i => TestEvent.NewTestEvent(i.ToString(), i.ToString()));
				Assert.Equal(99, (await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, events)).NextExpectedVersion);
			}
		}

		[Fact, Trait("Category", "Network")]
		public async Task returns_failure_status_when_conditionally_appending_with_version_mismatch() {
			const string stream = "returns_failure_status_when_conditionally_appending_with_version_mismatch";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var result = await store.ConditionalAppendToStreamAsync(stream, 7, new[] {TestEvent.NewTestEvent()});

				Assert.Equal(ConditionalWriteStatus.VersionMismatch, result.Status);
			}
		}

		[Fact, Trait("Category", "Network")]
		public async Task returns_success_status_when_conditionally_appending_with_matching_version() {
			const string stream = "returns_success_status_when_conditionally_appending_with_matching_version";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var result = await store
					.ConditionalAppendToStreamAsync(stream, ExpectedVersion.Any, new[] {TestEvent.NewTestEvent()});

				Assert.Equal(ConditionalWriteStatus.Succeeded, result.Status);
				Assert.NotNull(result.LogPosition);
				Assert.NotNull(result.NextExpectedVersion);
			}
		}

		[Fact, Trait("Category", "Network")]
		public async Task returns_failure_status_when_conditionally_appending_to_a_deleted_stream() {
			const string stream = "returns_failure_status_when_conditionally_appending_to_a_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

                await store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent());
                await store.DeleteStreamAsync(stream, ExpectedVersion.Any, true);

				var result = await store
					.ConditionalAppendToStreamAsync(stream, ExpectedVersion.Any, new[] {TestEvent.NewTestEvent()});

				Assert.Equal(ConditionalWriteStatus.StreamDeleted, result.Status);
			}
		}
	}

	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class ssl_append_to_stream : IClassFixture<ssl_append_to_stream.Fixture> {
		private readonly TcpType _tcpType = TcpType.Ssl;
		private readonly MiniNode _node;

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.To(node, _tcpType);
		}

		public ssl_append_to_stream(Fixture fixture) {
			_node = fixture.Node;
		}

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

		[Fact]
		public async Task should_allow_appending_zero_events_to_stream_with_no_problems() {
			const string stream = "should_allow_appending_zero_events_to_stream_with_no_problems";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
                Assert.Equal(-1,
	                (await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream)).NextExpectedVersion);

				var read = await store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
				Assert.Empty(read.Events);
			}
		}

		[Fact]
		public async Task should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist() {
			const string stream = "should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				Assert.Equal(0,  
					(await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent())).NextExpectedVersion);

				var read = await store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
				Assert.Single(read.Events);
			}
		}

		[Fact]
		public async Task should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist() {
			const string stream = "should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				Assert.Equal(0, 
					(await store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent())).NextExpectedVersion);

				var read = await store.ReadStreamEventsForwardAsync(stream, 0, 2, resolveLinkTos: false);
				Assert.Single(read.Events);
			}
		}

		[Fact]
		public async Task should_fail_writing_with_correct_exp_ver_to_deleted_stream() {
			const string stream = "should_fail_writing_with_correct_exp_ver_to_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				await Assert.ThrowsAsync<StreamDeletedException>(() => 
					store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent()));
			}
		}

		[Fact]
		public async Task should_fail_writing_with_any_exp_ver_to_deleted_stream() {
			const string stream = "should_fail_writing_with_any_exp_ver_to_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				await Assert.ThrowsAsync<StreamDeletedException>(() => 
					store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent()));
			}
		}

		[Fact]
		public async Task should_fail_writing_with_invalid_exp_ver_to_deleted_stream() {
			const string stream = "should_fail_writing_with_invalid_exp_ver_to_deleted_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				await Assert.ThrowsAsync<StreamDeletedException>(() =>
					store.AppendToStreamAsync(stream, 5, new[] {TestEvent.NewTestEvent()}));
			}
		}

		[Fact]
		public async Task should_append_with_correct_exp_ver_to_existing_stream() {
			const string stream = "should_append_with_correct_exp_ver_to_existing_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				Assert.Equal(0,  
					(await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent())).NextExpectedVersion);
				Assert.Equal(1, (await store.AppendToStreamAsync(stream, 0, TestEvent.NewTestEvent())).NextExpectedVersion);
			}
		}

		[Fact]
		public async Task should_append_with_any_exp_ver_to_existing_stream() {
			const string stream = "should_append_with_any_exp_ver_to_existing_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				Assert.Equal(0,  
					(await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent())).NextExpectedVersion);
				Assert.Equal(1,  (await store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent())).NextExpectedVersion);
			}
		}

		[Fact]
		public async Task should_return_log_position_when_writing() {
			const string stream = "should_return_log_position_when_writing";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var result = await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent());
				Assert.True(0 < result.LogPosition.PreparePosition);
				Assert.True(0 < result.LogPosition.CommitPosition);
			}
		}

		[Fact]
		public async Task should_fail_appending_with_wrong_exp_ver_to_existing_stream() {
			const string stream = "should_fail_appending_with_wrong_exp_ver_to_existing_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				Assert.Equal(0, 
					(await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent())).NextExpectedVersion);

				var wev = await Assert.ThrowsAsync<WrongExpectedVersionException>(() =>
					store.AppendToStreamAsync(stream, 1, TestEvent.NewTestEvent()));
				Assert.Equal(1, wev.ExpectedVersion);
				Assert.Equal(0, wev.ActualVersion);
			}
		}

		[Fact]
		public async Task can_append_multiple_events_at_once() {
			const string stream = "can_append_multiple_events_at_once";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var events = Enumerable.Range(0, 100).Select(i => TestEvent.NewTestEvent(i.ToString(), i.ToString()));
				Assert.Equal(99, (await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, events)).NextExpectedVersion);
			}
		}
	}
}
