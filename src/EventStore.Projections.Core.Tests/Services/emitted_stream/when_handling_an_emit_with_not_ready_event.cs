using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream {
	public class when_handling_an_emit_with_not_ready_event : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			AllWritesSucceed();
			NoOtherStreams();
		}

		public when_handling_an_emit_with_not_ready_event() {
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
		public void replies_with_await_message() {
			_stream.EmitEvents(
				new[] {
					new EmittedLinkTo(
						"test_stream", Guid.NewGuid(), "other_stream", CheckpointTag.FromPosition(0, 1100, 1000), null)
				});
			Assert.Equal(1, _readyHandler.HandledStreamAwaitingMessage.Count);
			Assert.Equal("test_stream", _readyHandler.HandledStreamAwaitingMessage[0].StreamId);
		}

		[Fact]
		public void processes_write_on_write_completed_if_ready() {
			var linkTo = new EmittedLinkTo(
				"test_stream", Guid.NewGuid(), "other_stream", CheckpointTag.FromPosition(0, 1100, 1000), null);
			_stream.EmitEvents(new[] {linkTo});
			linkTo.SetTargetEventNumber(1);
			_stream.Handle(new CoreProjectionProcessingMessage.EmittedStreamWriteCompleted("other_stream"));

			Assert.Equal(1, _readyHandler.HandledStreamAwaitingMessage.Count);
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.OfEventType(SystemEventTypes.LinkTo)
					.Count());
		}

		[Fact]
		public void replies_with_await_message_on_write_completed_if_not_yet_ready() {
			var linkTo = new EmittedLinkTo(
				"test_stream", Guid.NewGuid(), "other_stream", CheckpointTag.FromPosition(0, 1100, 1000), null);
			_stream.EmitEvents(new[] {linkTo});
			_stream.Handle(new CoreProjectionProcessingMessage.EmittedStreamWriteCompleted("one_more_stream"));

			Assert.Equal(2, _readyHandler.HandledStreamAwaitingMessage.Count);
			Assert.Equal("test_stream", _readyHandler.HandledStreamAwaitingMessage[0].StreamId);
			Assert.Equal("test_stream", _readyHandler.HandledStreamAwaitingMessage[1].StreamId);
		}
	}
}
