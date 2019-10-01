using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.projection_checkpoint {
	public class when_requesting_checkpoint_before_all_writes_completed : TestFixtureWithExistingEvents {
		private ProjectionCheckpoint _checkpoint;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			AllWritesSucceed();
			NoOtherStreams();
		}

		public when_requesting_checkpoint_before_all_writes_completed() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_checkpoint = new ProjectionCheckpoint(
				_bus, _ioDispatcher, new ProjectionVersion(1, 0, 0), null, _readyHandler,
				CheckpointTag.FromPosition(0, 100, 50), new TransactionFilePositionTagger(0), 250, 1);
			_checkpoint.Start();
			_checkpoint.ValidateOrderAndEmitEvents(
				new[] {
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream2", Guid.NewGuid(), "type", true, "data2", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream2", Guid.NewGuid(), "type", true, "data4", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
				});
			_checkpoint.ValidateOrderAndEmitEvents(
				new[] {
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream1", Guid.NewGuid(), "type", true, "data", null,
							CheckpointTag.FromPosition(0, 140, 130), null))
				});
			_checkpoint.ValidateOrderAndEmitEvents(
				new[] {
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream1", Guid.NewGuid(), "type", true, "data", null,
							CheckpointTag.FromPosition(0, 160, 150), null))
				});
			_checkpoint.Prepare(CheckpointTag.FromPosition(0, 200, 150));
		}

		[Fact]
		public void not_ready_for_checkpoint_immediately() {
			Assert.Equal(0,
				Consumer.HandledMessages.OfType<CoreProjectionProcessingMessage.ReadyForCheckpoint>().Count());
		}

		[Fact]
		public void ready_for_checkpoint_after_all_writes_complete() {
			var writes = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().ToArray();
			writes[0].Envelope.ReplyWith(new ClientMessage.WriteEventsCompleted(writes[0].CorrelationId, 0, 0, -1, -1));
			writes[1].Envelope.ReplyWith(new ClientMessage.WriteEventsCompleted(writes[1].CorrelationId, 0, 0, -1, -1));
			writes = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().ToArray();
			writes[2].Envelope.ReplyWith(new ClientMessage.WriteEventsCompleted(writes[2].CorrelationId, 0, 0, -1, -1));

			Assert.Equal(1,
				_readyHandler.HandledMessages.OfType<CoreProjectionProcessingMessage.ReadyForCheckpoint>().Count());
		}
	}
}
