using System;
using System.Text;
using EventStore.Core.Data;
using EventStore.Core.Util;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class when_receiving_a_committed_event_the_projection_should : TestFixtureWithCoreProjectionStarted {
		private Guid _eventId;

		protected override void Given() {
			TicksAreHandledImmediately();
			AllWritesSucceed();
			NoOtherStreams();
		}

		protected override void When() {
			//projection subscribes here
			_eventId = Guid.NewGuid();
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"/event_category/1", -1, "/event_category/1", -1, false, new TFPos(120, 110), _eventId,
						"handle_this_type", false, "data", "metadata"), _subscriptionId, 0));
		}

		[Fact]
		public void update_state_snapshot_at_correct_position() {
			Assert.Equal(1, _writeEventHandler.HandledMessages.OfEventType("Result").Count);

			var metedata =
				_writeEventHandler.HandledMessages.OfEventType("Result")[0].Metadata
					.ParseCheckpointTagVersionExtraJson(default(ProjectionVersion));

			Assert.Equal(120, metedata.Tag.CommitPosition);
			Assert.Equal(110, metedata.Tag.PreparePosition);
		}

		[Fact]
		public void pass_event_to_state_handler() {
			Assert.Equal(1, _stateHandler._eventsProcessed);
			Assert.Equal("/event_category/1", _stateHandler._lastProcessedStreamId);
			Assert.Equal("handle_this_type", _stateHandler._lastProcessedEventType);
			Assert.Equal(_eventId, _stateHandler._lastProcessedEventId);
			//TODO: support sequence numbers here
			Assert.Equal("metadata", _stateHandler._lastProcessedMetadata);
			Assert.Equal("data", _stateHandler._lastProcessedData);
		}
	}
}
