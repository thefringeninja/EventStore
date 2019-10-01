using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using System.Collections.Generic;
using EventStore.Core.Services.TimerService;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream.another_epoch {
	public class when_handling_a_timeout : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			AllWritesQueueUp();
			ExistingEvent("test_stream", "type1", @"{""v"": 1, ""c"": 100, ""p"": 50}", "data");
			ExistingEvent("test_stream", "type1", @"{""v"": 2, ""c"": 100, ""p"": 50}", "data");
		}

		private EmittedEvent[] CreateEventBatch() {
			return new EmittedEvent[] {
				new EmittedDataEvent(
					(string)"test_stream", Guid.NewGuid(), (string)"type1", (bool)true,
					(string)"data", (ExtraMetaData)null, CheckpointTag.FromPosition(0, 100, 50), (CheckpointTag)null,
					null),
				new EmittedDataEvent(
					(string)"test_stream", Guid.NewGuid(), (string)"type2", (bool)true,
					(string)"data", (ExtraMetaData)null, CheckpointTag.FromPosition(0, 100, 50), (CheckpointTag)null,
					null),
				new EmittedDataEvent(
					(string)"test_stream", Guid.NewGuid(), (string)"type3", (bool)true,
					(string)"data", (ExtraMetaData)null, CheckpointTag.FromPosition(0, 100, 50), (CheckpointTag)null,
					null)
			};
		}

		public when_handling_a_timeout() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_stream = new EmittedStream(
				"test_stream",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, maxWriteBatchLength: 50),
				new ProjectionVersion(1, 2, 2), new TransactionFilePositionTagger(0), CheckpointTag.Empty,
				_bus, _ioDispatcher, _readyHandler);
			_stream.Start();
			_stream.EmitEvents(CreateEventBatch());

			CompleteWriteWithResult(OperationResult.CommitTimeout);
		}

		[Fact]
		public void should_retry_the_write_with_the_same_events() {
			var current = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Last();
			while (Consumer.HandledMessages.Last() is TimerMessage.Schedule) {
				var message =
					Consumer.HandledMessages.Last() as TimerMessage.Schedule;
				message.Envelope.ReplyWith(message.ReplyMessage);

				CompleteWriteWithResult(OperationResult.CommitTimeout);

				var last = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Last();

				Assert.Equal(current.EventStreamId, last.EventStreamId);
				Assert.Equal(current.Events, last.Events);

				current = last;
			}

			Assert.Single(_readyHandler.HandledFailedMessages.OfType<CoreProjectionProcessingMessage.Failed>());
		}
	}
}
