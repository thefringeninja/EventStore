using System;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;
using EventStore.Projections.Core.Services;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class when_the_state_handler_does_not_process_event_the_projection_should :
		TestFixtureWithCoreProjectionStarted {
		protected override void Given() {
			ExistingEvent(
				"$projections-projection-result", "Result", @"{""c"": 100, ""p"": 50}", "{}");
			ExistingEvent(
				"$projections-projection-checkpoint", ProjectionEventTypes.ProjectionCheckpoint,
				@"{""c"": 100, ""p"": 50}", "{}");
		}

		protected override void When() {
			//projection subscribes here
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"/event_category/1", -1, "/event_category/1", -1, false, new TFPos(120, 110),
						new TFPos(120, 110),
						Guid.NewGuid(), "skip_this_type", false, new byte[0], new byte[0], null, null,
						default(DateTime)),
					_subscriptionId, 0));
		}

		[Fact]
		public void not_update_state_snapshot() {
			Assert.Equal(0, _writeEventHandler.HandledMessages.Count);
		}
	}
}
