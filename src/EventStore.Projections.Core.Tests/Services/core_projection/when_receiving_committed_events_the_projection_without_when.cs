using System;
using System.Linq;
using System.Text;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public abstract class specification_with_query_without_when : TestFixtureWithCoreProjectionStarted {
		protected Guid _eventId;

		protected override bool GivenCheckpointsEnabled() {
			return false;
		}

		protected override bool GivenEmitEventEnabled() {
			return false;
		}

		protected override bool GivenStopOnEof() {
			return true;
		}

		protected override int GivenPendingEventsThreshold() {
			return 0;
		}

		protected override ProjectionProcessingStrategy GivenProjectionProcessingStrategy() {
			return CreateQueryProcessingStrategy();
		}

		protected override void Given() {
			_checkpointHandledThreshold = 0;
			_checkpointUnhandledBytesThreshold = 0;
			_configureBuilderByQuerySource = source => {
				source.FromAll();
				source.AllEvents();
				source.NoWhen();
			};
			TicksAreHandledImmediately();
			NoOtherStreams();
			AllWritesSucceed();
		}
	}

	public class when_receiving_committed_events_the_projection_without_when : specification_with_query_without_when {
		protected override void When() {
			//projection subscribes here
			_eventId = Guid.NewGuid();
			Consumer.HandledMessages.Clear();
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"account-01", 1, "account-01", 1, false, new TFPos(120, 110), _eventId, "handle_this_type",
						false, "data1", "metadata"), _subscriptionId, 0));
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"account-02", 2, "account-02", 2, false, new TFPos(140, 130), _eventId, "handle_this_type",
						false, "data2", "metadata"), _subscriptionId, 1));
		}

		[Fact]
		public void update_state_snapshots_are_written_to_the_correct_stream() {
			var writeEvents =
				_writeEventHandler.HandledMessages.Where(v => v.Events.Any(e => e.EventType == "Result")).ToList();
			Assert.Equal(2, writeEvents.Count);
			Assert.Equal("$projections-projection-result", writeEvents[0].EventStreamId);
			Assert.Equal("$projections-projection-result", writeEvents[1].EventStreamId);
			Assert.Equal("data1", Encoding.UTF8.GetString(writeEvents[0].Events[0].Data));
			Assert.Equal("data2", Encoding.UTF8.GetString(writeEvents[1].Events[0].Data));
		}
	}


	public class
		when_handling_event_does_not_change_state_the_projection_without_when : specification_with_query_without_when {
		protected override void When() {
			//projection subscribes here
			_eventId = Guid.NewGuid();
			Consumer.HandledMessages.Clear();
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"account-01", 1, "account-01", 1, false, new TFPos(120, 110), _eventId, "handle_this_type",
						false, "data1", "metadata"), _subscriptionId, 0));
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"account-01", 2, "account-01", 2, false, new TFPos(140, 130), _eventId, "handle_this_type",
						false, "data1", "metadata"), _subscriptionId, 1));
		}

		[Fact(Skip = "To be fixed")]
		public void result_events_are_produced_for_each_received_event() {
			var writeEvents =
				_writeEventHandler.HandledMessages.Where(v => v.Events.Any(e => e.EventType == "Result")).ToList();
			Assert.Equal(2, writeEvents.Count);
			Assert.Equal("$projections-projection-result", writeEvents[0].EventStreamId);
			Assert.Equal("$projections-projection-result", writeEvents[1].EventStreamId);
			Assert.Equal("data1", Encoding.UTF8.GetString(writeEvents[0].Events[0].Data));
			Assert.Equal("data1", Encoding.UTF8.GetString(writeEvents[1].Events[0].Data));
		}

		[Fact]
		public void no_result_removed_events_are_produced() {
			var writeEvents =
				_writeEventHandler.HandledMessages.Where(v => v.Events.Any(e => e.EventType == "ResultRemoved"))
					.ToList();
			Assert.Equal(0, writeEvents.Count);
		}
	}
}
