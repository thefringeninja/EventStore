using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.projection_checkpoint {
	public class when_emitting_events_the_non_started_checkpoint : TestFixtureWithExistingEvents {
		private ProjectionCheckpoint _checkpoint;
		private TestCheckpointManagerMessageHandler _readyHandler;

		public when_emitting_events_the_non_started_checkpoint() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_checkpoint = new ProjectionCheckpoint(
				_bus, _ioDispatcher, new ProjectionVersion(1, 0, 0), null, _readyHandler,
				CheckpointTag.FromPosition(0, 100, 50), new TransactionFilePositionTagger(0), 250, 1);
			_checkpoint.ValidateOrderAndEmitEvents(
				new[] {
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream2", Guid.NewGuid(), "type", true, "data2", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream3", Guid.NewGuid(), "type", true, "data3", null,
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
		}

		[Fact]
		public void does_not_publish_write_events() {
			Assert.Equal(0, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
		}
	}
}
