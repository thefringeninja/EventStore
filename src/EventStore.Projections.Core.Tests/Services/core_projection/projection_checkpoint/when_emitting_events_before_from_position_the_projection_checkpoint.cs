using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.projection_checkpoint {
	public class when_emitting_events_before_from_position_the_projection_checkpoint : TestFixtureWithExistingEvents {
		private ProjectionCheckpoint _checkpoint;
		private Exception _lastException;
		private TestCheckpointManagerMessageHandler _readyHandler;

		public when_emitting_events_before_from_position_the_projection_checkpoint() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_checkpoint = new ProjectionCheckpoint(
				_bus, _ioDispatcher, new ProjectionVersion(1, 0, 0), null, _readyHandler,
				CheckpointTag.FromPosition(0, 100, 50), new TransactionFilePositionTagger(0), 250, 1);
			try {
				_checkpoint.ValidateOrderAndEmitEvents(
					new[] {
						new EmittedEventEnvelope(
							new EmittedDataEvent(
								"stream1", Guid.NewGuid(), "type", true, "data", null,
								CheckpointTag.FromPosition(0, 40, 30), null))
					});
			} catch (Exception ex) {
				_lastException = ex;
			}
		}

		[Fact]
		public void throws_invalid_operation_exception() {
			Assert.Throws<InvalidOperationException>(() => {
				if (_lastException != null) throw _lastException;
			});
		}
	}
}
