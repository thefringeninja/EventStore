using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Data;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;
using ExpectedVersion = EventStore.ClientAPI.ExpectedVersion;

namespace EventStore.Core.Tests.ClientAPI {
	[TestFixture, Category("ClientAPI"), Category("LongRunning")]
	public class when_working_with_stream_metadata_as_byte_array : SpecificationWithDirectoryPerTestFixture {
		private MiniNode _node;
		private IEventStoreConnection _connection;

		[OneTimeSetUp]
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

		[OneTimeTearDown]
		public override Task TestFixtureTearDown() {
			_connection.Close();
			_node.Shutdown();
			return base.TestFixtureTearDown();
		}

		[Test]
		public async Task setting_empty_metadata_worksAsync() {
			const string stream = "setting_empty_metadata_works";

            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, (byte[])null);

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(false, meta.IsStreamDeleted);
			Assert.AreEqual(0, meta.MetastreamVersion);
			Assert.AreEqual(new byte[0], meta.StreamMetadata);
		}

		[Test]
		public async Task setting_metadata_few_times_returns_last_metadataAsync() {
			const string stream = "setting_metadata_few_times_returns_last_metadata";

			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes);
			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(false, meta.IsStreamDeleted);
			Assert.AreEqual(0, meta.MetastreamVersion);
			Assert.AreEqual(metadataBytes, meta.StreamMetadata);

			metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, 0, metadataBytes);
			meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(false, meta.IsStreamDeleted);
			Assert.AreEqual(1, meta.MetastreamVersion);
			Assert.AreEqual(metadataBytes, meta.StreamMetadata);
		}

		[Test]
		public void trying_to_set_metadata_with_wrong_expected_version_fails() {
			const string stream = "trying_to_set_metadata_with_wrong_expected_version_fails";
			Assert.That(() => _connection.SetStreamMetadataAsync(stream, 5, new byte[100]).Wait(),
				Throws.Exception.InstanceOf<AggregateException>()
					.With.InnerException.InstanceOf<WrongExpectedVersionException>());
		}

		[Test]
		public async Task setting_metadata_with_expected_version_any_worksAsync() {
			const string stream = "setting_metadata_with_expected_version_any_works";

			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.Any, metadataBytes);
			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(false, meta.IsStreamDeleted);
			Assert.AreEqual(0, meta.MetastreamVersion);
			Assert.AreEqual(metadataBytes, meta.StreamMetadata);

			metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.Any, metadataBytes);
			meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(false, meta.IsStreamDeleted);
			Assert.AreEqual(1, meta.MetastreamVersion);
			Assert.AreEqual(metadataBytes, meta.StreamMetadata);
		}

		[Test]
		public async Task setting_metadata_for_not_existing_stream_worksAsync() {
			const string stream = "setting_metadata_for_not_existing_stream_works";
			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes);

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(false, meta.IsStreamDeleted);
			Assert.AreEqual(0, meta.MetastreamVersion);
			Assert.AreEqual(metadataBytes, meta.StreamMetadata);
		}

		[Test]
		public async Task setting_metadata_for_existing_stream_worksAsync() {
			const string stream = "setting_metadata_for_existing_stream_works";

            await _connection.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent(),
				TestEvent.NewTestEvent());

			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes);

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(false, meta.IsStreamDeleted);
			Assert.AreEqual(0, meta.MetastreamVersion);
			Assert.AreEqual(metadataBytes, meta.StreamMetadata);
		}

		[Test]
		public void setting_metadata_for_deleted_stream_throws_stream_deleted_exception() {
			const string stream = "setting_metadata_for_deleted_stream_throws_stream_deleted_exception";

			_connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true).Wait();

			var metadataBytes = Guid.NewGuid().ToByteArray();
			Assert.That(
				() => _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes).Wait(),
				Throws.Exception.InstanceOf<AggregateException>()
					.With.InnerException.InstanceOf<StreamDeletedException>());
		}

		[Test]
		public async Task getting_metadata_for_nonexisting_stream_returns_empty_byte_arrayAsync() {
			const string stream = "getting_metadata_for_nonexisting_stream_returns_empty_byte_array";

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(false, meta.IsStreamDeleted);
			Assert.AreEqual(-1, meta.MetastreamVersion);
			Assert.AreEqual(new byte[0], meta.StreamMetadata);
		}

		[Test]
		public async Task getting_metadata_for_deleted_stream_returns_empty_byte_array_and_signals_stream_deletionAsync() {
			const string stream =
				"getting_metadata_for_deleted_stream_returns_empty_byte_array_and_signals_stream_deletion";

			var metadataBytes = Guid.NewGuid().ToByteArray();
            await _connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream, metadataBytes);

            await _connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);

			var meta = await _connection.GetStreamMetadataAsRawBytesAsync(stream);
			Assert.AreEqual(stream, meta.Stream);
			Assert.AreEqual(true, meta.IsStreamDeleted);
			Assert.AreEqual(EventNumber.DeletedStream, meta.MetastreamVersion);
			Assert.AreEqual(new byte[0], meta.StreamMetadata);
		}
	}
}
