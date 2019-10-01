using System;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;
using EventStore.Projections.Core.Services;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using System.Text;
using EventStore.Common.Utils;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class
		when_stopping_a_projection_with_existing_state_without_updating_the_state :
			TestFixtureWithCoreProjectionStarted {
		private string _testProjectionState = @"{""test"":1}";

		protected override void Given() {
			//write existing checkpoint
			ExistingEvent(
				"$projections-projection-checkpoint", ProjectionEventTypes.ProjectionCheckpoint,
				@"{""c"": 100, ""p"": 50}", _testProjectionState);

			AllWritesQueueUp();
		}

		protected override void When() {
			//force write of another checkpoint
			_bus.Publish(
				new EventReaderSubscriptionMessage.CheckpointSuggested(
					_subscriptionId, CheckpointTag.FromPosition(0, 160, 150), 77.7f, 0));

			_coreProjection.Stop();
		}

		[Fact]
		public void a_projection_checkpoint_event_is_published() {
			AllWriteComplete();
			Assert.Equal(
				1,
				_writeEventHandler.HandledMessages.Count(v =>
					v.Events.Any(e => e.EventType == ProjectionEventTypes.ProjectionCheckpoint)));
			Assert.Equal(1, Consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Stopped>().Count());
		}

		[Fact]
		public void previous_state_is_saved_in_checkpoint_event() {
			AllWriteComplete();
			Assert.Equal(
				1,
				_writeEventHandler.HandledMessages.Count(
					v => v.Events.Any(
						e => e.EventType == ProjectionEventTypes.ProjectionCheckpoint
						     && Helper.UTF8NoBom.GetString(e.Data).Equals("[" + _testProjectionState + "]")
					)
				)
			);
		}
	}
}
