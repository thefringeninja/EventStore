using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;
using EventStore.Projections.Core.Services;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class
		when_the_state_handler_fails_to_load_state_the_projection_should : TestFixtureWithCoreProjectionStarted {
		protected override void Given() {
			ExistingEvent(
				"$projections-projection-result", "Result", @"{""c"": 100, ""p"": 50}", "{}");
			ExistingEvent(
				"$projections-projection-checkpoint", ProjectionEventTypes.ProjectionCheckpoint,
				@"{""c"": 100, ""p"": 50}", "{}");
			NoStream("$projections-projection-order");
			AllWritesToSucceed("$projections-projection-order");
		}

		protected override FakeProjectionStateHandler GivenProjectionStateHandler() {
			return new FakeProjectionStateHandler(failOnLoad: true);
		}

		protected override void When() {
			//projection subscribes here
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"/event_category/1", -1, "/event_category/1", -1, false, new TFPos(120, 110),
						Guid.NewGuid(), "handle_this_type", false, "data",
						"metadata"), _subscriptionId, 0));
		}

		[Fact]
		public void should_publish_faulted_message() {
			Assert.Equal(1, Consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Faulted>().Count());
		}

		[Fact]
		public void not_emit_a_state_updated_event() {
			Assert.Equal(0, _writeEventHandler.HandledMessages.OfEventType("StateUpdate").Count());
		}
	}
}
