using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream {
	public class when_handling_an_emit_with_stream_metadata_to_empty_stream : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;
		private EmittedStream.WriterConfiguration.StreamMetadata _streamMetadata;
		private EmittedStream.WriterConfiguration _writerConfiguration;

		protected override void Given() {
			AllWritesQueueUp();
			NoStream("test_stream");
			_streamMetadata = new EmittedStream.WriterConfiguration.StreamMetadata(maxCount: 10);
			_writerConfiguration = new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
				_streamMetadata, null, maxWriteBatchLength: 50);
		}

		public when_handling_an_emit_with_stream_metadata_to_empty_stream() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_stream = new EmittedStream(
				"test_stream", _writerConfiguration, new ProjectionVersion(1, 0, 0),
				new TransactionFilePositionTagger(0),
				CheckpointTag.FromPosition(0, 40, 30), _bus, _ioDispatcher, _readyHandler);
			_stream.Start();
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), null)
				});
		}

		[Fact]
		public void publishes_write_stream_metadata() {
			Assert.Equal(
				1, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().ToStream("$$test_stream").Count());
		}

		[Fact]
		public void does_not_write_stream_metadata_second_time() {
			OneWriteCompletes();
			OneWriteCompletes();
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 400, 350), null)
				});
			Assert.Equal(
				1, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().ToStream("$$test_stream").Count());
		}

		[Fact]
		public void publishes_write_emitted_event_on_write_stream_metadata_completed() {
			OneWriteCompletes();
			Assert.Equal(
				1, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().ToStream("test_stream").Count());
		}

		[Fact]
		public void does_not_reply_with_write_completed_message() {
			Assert.Equal(0, _readyHandler.HandledWriteCompletedMessage.Count);
		}

		[Fact]
		public void reply_with_write_completed_message_when_write_completes() {
			OneWriteCompletes();
			OneWriteCompletes();
			Assert.True(_readyHandler.HandledWriteCompletedMessage.Any(v => v.StreamId == "test_stream"));
			// more than one is ok
		}
	}
}
