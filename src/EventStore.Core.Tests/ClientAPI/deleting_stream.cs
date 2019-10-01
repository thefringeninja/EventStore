using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class deleting_stream : SpecificationWithDirectoryPerTestFixture {
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
		public async Task which_doesnt_exists_should_success_when_passed_empty_stream_expected_version() {
			const string stream = "which_already_exists_should_success_when_passed_empty_stream_expected_version";
			using (var connection = BuildConnection(_node)) {
                await connection.ConnectAsync();
				await connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task which_doesnt_exists_should_success_when_passed_any_for_expected_version() {
			const string stream = "which_already_exists_should_success_when_passed_any_for_expected_version";
			using (var connection = BuildConnection(_node)) {
                await connection.ConnectAsync();

				await connection.DeleteStreamAsync(stream, ExpectedVersion.Any, hardDelete: true);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task with_invalid_expected_version_should_fail() {
			const string stream = "with_invalid_expected_version_should_fail";
			using (var connection = BuildConnection(_node)) {
                await connection.ConnectAsync();

                await Assert.ThrowsAsync<WrongExpectedVersionException>(() =>
	                connection.DeleteStreamAsync(stream, 1, hardDelete: true));
			}
		}

		public async Task should_return_log_position_when_writing() {
			const string stream = "delete_should_return_log_position_when_writing";
			using (var connection = BuildConnection(_node)) {
				await connection.ConnectAsync();

				var result = await connection
					.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent());
				var delete = await connection.DeleteStreamAsync(stream, 1, hardDelete: true);

				Assert.True(0 < result.LogPosition.PreparePosition);
				Assert.True(0 < result.LogPosition.CommitPosition);
			}
		}

		[Fact]
		[Trait("Category", "Network")]
		public async Task which_was_already_deleted_should_fail() {
			const string stream = "which_was_allready_deleted_should_fail";
			using (var connection = BuildConnection(_node)) {
				await connection.ConnectAsync();

				await connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

				await Assert.ThrowsAsync<StreamDeletedException>(
					() => connection.DeleteStreamAsync(stream, ExpectedVersion.Any, hardDelete: true));
			}
		}
	}
}
