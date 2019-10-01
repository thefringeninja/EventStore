using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream {
	public class
		when_handling_an_emit_with_expected_tag_the_started_in_recovery_stream : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			ExistingEvent("test_stream", "type", @"{""c"": 100, ""p"": 50}", "data");
		}

		public when_handling_an_emit_with_expected_tag_the_started_in_recovery_stream() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_stream = new EmittedStream(
				"test_stream",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, maxWriteBatchLength: 50),
				new ProjectionVersion(1, 0, 0), new TransactionFilePositionTagger(0),
				CheckpointTag.FromPosition(0, 0, -1),
				_bus, _ioDispatcher, _readyHandler);
			_stream.Start();
		}

		[Fact]
		public void does_not_publish_already_published_events() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 100, 50), CheckpointTag.FromPosition(0, 40, 20))
				});
			Assert.Equal(0, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
		}

		[Fact]
		public void publishes_not_yet_published_events_if_expected_tag_is_the_same() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), CheckpointTag.FromPosition(0, 100, 50))
				});
			Assert.Equal(1, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
		}

		[Fact]
		public void does_not_publish_not_yet_published_events_if_expected_tag_is_before_last_event_tag() {
			//TODO: is it corrupted dB case? 
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), CheckpointTag.FromPosition(0, 40, 20))
				});
			Assert.Equal(0, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
		}

		[Fact]
		public void correct_stream_id_is_set_on_write_events_message() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), CheckpointTag.FromPosition(0, 100, 50))
				});
			Assert.Equal(
				"test_stream", Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Single().EventStreamId);
		}

		[Fact]
		public void metadata_include_commit_and_prepare_positions() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), CheckpointTag.FromPosition(0, 100, 50))
				});
			var metaData =
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Single().Events[0].Metadata
					.ParseCheckpointTagVersionExtraJson(default(ProjectionVersion));
			Assert.Equal(200, metaData.Tag.CommitPosition);
			Assert.Equal(150, metaData.Tag.PreparePosition);
		}
	}
}
