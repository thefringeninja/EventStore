using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream {
	public class when_handling_an_emit_to_the_nonexisting_stream : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			AllWritesQueueUp();
			AllWritesToSucceed("$$test_stream");
			NoOtherStreams();
		}

		public when_handling_an_emit_to_the_nonexisting_stream() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_stream = new EmittedStream(
				"test_stream",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, maxWriteBatchLength: 50),
				new ProjectionVersion(1, 0, 0), new TransactionFilePositionTagger(0),
				CheckpointTag.FromPosition(0, 40, 30),
				_bus, _ioDispatcher, _readyHandler);
			_stream.Start();
		}

		[Fact]
		public void throws_if_position_is_prior_to_from_position() {
			Assert.Throws<InvalidOperationException>(() => {
				_stream.EmitEvents(
					new[] {
						new EmittedDataEvent(
							"test_stream", Guid.NewGuid(), "type", true, "data", null,
							CheckpointTag.FromPosition(0, 20, 10), null)
					});
			});
		}

		[Fact]
		public void publishes_already_published_events() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 100, 50), null)
				});
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.ExceptOfEventType(SystemEventTypes.StreamMetadata)
					.Count());
		}

		[Fact]
		public void publishes_not_yet_published_events() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), null)
				});
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.ExceptOfEventType(SystemEventTypes.StreamMetadata)
					.Count());
		}

		[Fact]
		public void does_not_reply_with_write_completed_message() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), null)
				});
			Assert.Equal(0, _readyHandler.HandledWriteCompletedMessage.Count);
		}

		[Fact]
		public void reply_with_write_completed_message_when_write_completes() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 200, 150), null)
				});
			OneWriteCompletes();
			Assert.True(_readyHandler.HandledWriteCompletedMessage.Any(v => v.StreamId == "test_stream"));
			// more than one is ok
		}
	}
}
