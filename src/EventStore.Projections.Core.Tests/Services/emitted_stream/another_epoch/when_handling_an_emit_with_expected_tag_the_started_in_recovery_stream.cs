using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream.another_epoch {
	public class
		when_handling_an_emit_with_expected_tag_the_started_in_recovery_stream : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			ExistingEvent("test_stream", "type", @"{""v"": 1, ""c"": 100, ""p"": 50}", "data");
			AllWritesSucceed();
			NoOtherStreams();
		}

		public when_handling_an_emit_with_expected_tag_the_started_in_recovery_stream() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_stream = new EmittedStream(
				"test_stream",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, maxWriteBatchLength: 50),
				new ProjectionVersion(1, 2, 2), new TransactionFilePositionTagger(0),
				CheckpointTag.FromPosition(0, 0, -1),
				_bus, _ioDispatcher, _readyHandler);
			_stream.Start();
		}

		[Fact]
		public void requests_restart_if_different_smaller_tag() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 100, 50), CheckpointTag.FromPosition(0, 40, 20))
				});
			Assert.Equal(
				0,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.ExceptOfEventType(SystemEventTypes.StreamMetadata)
					.Count());
			Assert.Equal(1, _readyHandler.HandledRestartRequestedMessages.Count());
		}

		[Fact]
		public void publishes_all_events_even_with_smaller_tag() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 40, 20), null)
				});
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.ExceptOfEventType(SystemEventTypes.StreamMetadata)
					.Count());
		}

		[Fact]
		public void requests_restart_even_if_expected_tag_is_the_same_but_epoch() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), CheckpointTag.FromPosition(0, 100, 50))
				});
			Assert.Equal(
				0,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.ExceptOfEventType(SystemEventTypes.StreamMetadata)
					.Count());
			Assert.Equal(1, _readyHandler.HandledRestartRequestedMessages.Count());
		}

		[Fact]
		public void metadata_include_correct_version() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), null)
				});
			var metaData =
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.OfEventType("type")
					.Single()
					.Metadata.ParseCheckpointTagVersionExtraJson(default(ProjectionVersion));
			Assert.Equal(new ProjectionVersion(1, 2, 2), metaData.Version);
		}
	}
}
