using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream {
	public class when_checkpoint_requested_with_pending_writes : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			base.Given();
			AllWritesSucceed();
			NoOtherStreams();
		}

		public when_checkpoint_requested_with_pending_writes() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			;
			_stream = new EmittedStream(
				"test",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, 50), new ProjectionVersion(1, 0, 0),
				new TransactionFilePositionTagger(0), CheckpointTag.FromPosition(0, 0, -1), _bus, _ioDispatcher,
				_readyHandler);
			_stream.Start();
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test", Guid.NewGuid(), "type", true, "data", null, CheckpointTag.FromPosition(0, 100, 50),
						null)
				});
			_stream.Checkpoint();
		}

		[Fact]
		public void does_not_publish_ready_for_checkpoint_immediately() {
			Assert.Equal(
				0, Consumer.HandledMessages.OfType<CoreProjectionProcessingMessage.ReadyForCheckpoint>().Count());
		}

		[Fact]
		public void publishes_ready_for_checkpoint_on_handling_last_write_events_completed() {
			var msg = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().First();
			_bus.Publish(new ClientMessage.WriteEventsCompleted(msg.CorrelationId, 0, 0, -1, -1));
			Assert.Equal(
				1, _readyHandler.HandledMessages.OfType<CoreProjectionProcessingMessage.ReadyForCheckpoint>().Count());
		}
	}
}
