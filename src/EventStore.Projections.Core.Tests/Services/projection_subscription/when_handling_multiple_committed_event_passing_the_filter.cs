using System;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_subscription {
	public class when_handling_multiple_committed_event_passing_the_filter : TestFixtureWithProjectionSubscription {
		protected override void Given() {
			base.Given();
			_checkpointProcessedEventsThreshold = 2;
		}

		protected override void When() {
			_subscription.Handle(
				ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
					Guid.NewGuid(), new TFPos(200, 150), "test-stream", 1, false, Guid.NewGuid(),
					"bad-event-type", false, new byte[0], new byte[0]));
			_subscription.Handle(
				ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
					Guid.NewGuid(), new TFPos(300, 250), "test-stream", 2, false, Guid.NewGuid(),
					"bad-event-type", false, new byte[0], new byte[0]));
		}

		[Fact]
		public void events_passed_to_downstream_handler_have_correct_subscription_sequence_numbers() {
			Assert.Equal(2, _eventHandler.HandledMessages.Count);

			Assert.Equal(0, _eventHandler.HandledMessages[0].SubscriptionMessageSequenceNumber);
			Assert.Equal(1, _eventHandler.HandledMessages[1].SubscriptionMessageSequenceNumber);
		}

		[Fact]
		public void suggests_a_checkpoint() {
			Assert.Equal(1, _checkpointHandler.HandledMessages.Count);
			Assert.Equal(CheckpointTag.FromPosition(0, 300, 250),
				_checkpointHandler.HandledMessages[0].CheckpointTag);
		}
	}
}
