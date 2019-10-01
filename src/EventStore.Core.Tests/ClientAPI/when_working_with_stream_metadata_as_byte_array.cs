using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Data;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using ExpectedVersion = EventStore.ClientAPI.ExpectedVersion;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class when_working_with_stream_metadata_as_byte_array : SpecificationWithDirectoryPerTestFixture {
		private MiniNode _node;
		private IEventStoreConnection _connection;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName);
			await _node.Start();

			_connection = BuildConnection(_node);
			await _connection.ConnectAsync();
		}

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		public override async Task TestFixtureTearDown() {
			_connection.Close();
			await _node.Shutdown();
			await base.TestFixtureTearDown();
		}

		[Fact]
		public async Task setting_empty_metadata_works() {
			const string stream = "setting_empty_metadata_works";

            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, (byte[])null);

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.False(meta.IsStreamDeleted);
			Assert.Equal(0, meta.MetastreamVersion);
			Assert.Equal(new byte[0], meta.StreamMetadata);
		}

		[Fact]
		public async Task setting_metadata_few_times_returns_last_metadata() {
			const string stream = "setting_metadata_few_times_returns_last_metadata";

			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes);
			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.False(meta.IsStreamDeleted);
			Assert.Equal(0, meta.MetastreamVersion);
			Assert.Equal(metadataBytes, meta.StreamMetadata);

			metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, 0, metadataBytes);
			meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.False(meta.IsStreamDeleted);
			Assert.Equal(1, meta.MetastreamVersion);
			Assert.Equal(metadataBytes, meta.StreamMetadata);
		}

		[Fact]
		public async Task trying_to_set_metadata_with_wrong_expected_version_fails() {
			const string stream = "trying_to_set_metadata_with_wrong_expected_version_fails";
			await Assert.ThrowsAsync<WrongExpectedVersionException>(() => _connection.SetStreamMetadataAsync(stream, 5, new byte[100]));
		}

		[Fact]
		public async Task setting_metadata_with_expected_version_any_works() {
			const string stream = "setting_metadata_with_expected_version_any_works";

			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.Any, metadataBytes);
			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.False(meta.IsStreamDeleted);
			Assert.Equal(0, meta.MetastreamVersion);
			Assert.Equal(metadataBytes, meta.StreamMetadata);

			metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.Any, metadataBytes);
			meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.False(meta.IsStreamDeleted);
			Assert.Equal(1, meta.MetastreamVersion);
			Assert.Equal(metadataBytes, meta.StreamMetadata);
		}

		[Fact]
		public async Task setting_metadata_for_not_existing_stream_works() {
			const string stream = "setting_metadata_for_not_existing_stream_works";
			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes);

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.False(meta.IsStreamDeleted);
			Assert.Equal(0, meta.MetastreamVersion);
			Assert.Equal(metadataBytes, meta.StreamMetadata);
		}

		[Fact]
		public async Task setting_metadata_for_existing_stream_works() {
			const string stream = "setting_metadata_for_existing_stream_works";

            await _connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(),
				TestEvent.NewTestEvent());

			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes);

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.False(meta.IsStreamDeleted);
			Assert.Equal(0, meta.MetastreamVersion);
			Assert.Equal(metadataBytes, meta.StreamMetadata);
		}

		[Fact]
		public async Task setting_metadata_for_deleted_stream_throws_stream_deleted_exception() {
			const string stream = "setting_metadata_for_deleted_stream_throws_stream_deleted_exception";

            await _connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

			var metadataBytes = Guid.NewGuid().ToByteArray();
			await Assert.ThrowsAsync<StreamDeletedException>(
				() => _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes));
		}

		[Fact]
		public async Task getting_metadata_for_nonexisting_stream_returns_empty_byte_array() {
			const string stream = "getting_metadata_for_nonexisting_stream_returns_empty_byte_array";

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.False(meta.IsStreamDeleted);
			Assert.Equal(-1, meta.MetastreamVersion);
			Assert.Equal(new byte[0], meta.StreamMetadata);
		}

		[Fact]
		public async Task getting_metadata_for_deleted_stream_returns_empty_byte_array_and_signals_stream_deletion() {
			const string stream =
				"getting_metadata_for_deleted_stream_returns_empty_byte_array_and_signals_stream_deletion";

			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes);

            await _connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.Equal(stream, meta.Stream);
			Assert.Equal(true, meta.IsStreamDeleted);
			Assert.Equal(EventNumber.DeletedStream, meta.MetastreamVersion);
			Assert.Equal(new byte[0], meta.StreamMetadata);
		}
	}
}
