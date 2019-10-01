using System;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;
using EventStore.Projections.Core.Services;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class when_starting_an_existing_projection : TestFixtureWithCoreProjectionStarted {
		private string _testProjectionState = @"{""test"":1}";

		protected override void Given() {
			ExistingEvent(
				"$projections-projection-result", "Result",
				@"{""c"": 100, ""p"": 50}", _testProjectionState);
			ExistingEvent(
				"$projections-projection-checkpoint", ProjectionEventTypes.ProjectionCheckpoint,
				@"{""c"": 100, ""p"": 50}", _testProjectionState);
			ExistingEvent(
				"$projections-projection-result", "Result",
				@"{""c"": 200, ""p"": 150}", _testProjectionState);
			ExistingEvent(
				"$projections-projection-result", "Result",
				@"{""c"": 300, ""p"": 250}", _testProjectionState);
		}

		protected override void When() {
		}


		[Fact]
		public void should_subscribe_from_the_last_known_checkpoint_position() {
			Assert.Equal(1, _subscribeProjectionHandler.HandledMessages.Count);
			Assert.Equal(100, _subscribeProjectionHandler.HandledMessages[0].FromPosition.Position.CommitPosition);
			Assert.Equal(50, _subscribeProjectionHandler.HandledMessages[0].FromPosition.Position.PreparePosition);
		}

		[Fact]
		public void should_publish_started_message() {
			Assert.Equal(1, Consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Started>().Count());
			var startedMessage = Consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Started>().Single();
			Assert.Equal(_projectionCorrelationId, startedMessage.ProjectionId);
		}
	}
}
