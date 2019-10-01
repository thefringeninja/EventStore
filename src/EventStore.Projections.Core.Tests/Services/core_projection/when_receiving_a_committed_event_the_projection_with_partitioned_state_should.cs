using System;
using System.Linq;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Util;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class when_receiving_a_committed_event_the_projection_with_partitioned_state_should :
		TestFixtureWithCoreProjectionStarted {
		private Guid _eventId;

		protected override void Given() {
			_configureBuilderByQuerySource = source => {
				source.FromAll();
				source.AllEvents();
				source.SetByStream();
				source.SetDefinesStateTransform();
			};
			TicksAreHandledImmediately();
			AllWritesSucceed();
			NoOtherStreams();
		}

		protected override void When() {
			//projection subscribes here
			_eventId = Guid.NewGuid();
			Consumer.HandledMessages.Clear();
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"account-01", -1, "account-01", -1, false, new TFPos(120, 110), _eventId,
						"handle_this_type", false, "data", "metadata"), _subscriptionId, 0));
		}

		[Fact]
		public void request_partition_state_from_the_correct_stream() {
			// 1 - for load state
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsBackward>()
					.Count(v => v.EventStreamId == "$projections-projection-account-01-checkpoint"));
		}

		[Fact]
		public void update_state_snapshot_is_written_to_the_correct_stream() {
			var writeEvents =
				_writeEventHandler.HandledMessages.Where(v => v.Events.Any(e => e.EventType == "Result")).ToList();
			Assert.Equal(1, writeEvents.Count);

			var message = writeEvents[0];
			Assert.Equal("$projections-projection-account-01-result", message.EventStreamId);
		}

		[Fact]
		public void update_state_snapshot_at_correct_position() {
			var writeEvents =
				_writeEventHandler.HandledMessages.Where(v => v.Events.Any(e => e.EventType == "Result")).ToList();
			Assert.Equal(1, writeEvents.Count);

			var metedata = writeEvents[0].Events[0].Metadata
				.ParseCheckpointTagVersionExtraJson(default(ProjectionVersion));

			Assert.Equal(120, metedata.Tag.CommitPosition);
			Assert.Equal(110, metedata.Tag.PreparePosition);
		}

		[Fact]
		public void pass_event_to_state_handler() {
			Assert.Equal(1, _stateHandler._eventsProcessed);
			Assert.Equal("account-01", _stateHandler._lastProcessedStreamId);
			Assert.Equal("handle_this_type", _stateHandler._lastProcessedEventType);
			Assert.Equal(_eventId, _stateHandler._lastProcessedEventId);
			//TODO: support sequence numbers here
			Assert.Equal("metadata", _stateHandler._lastProcessedMetadata);
			Assert.Equal("data", _stateHandler._lastProcessedData);
		}

		[Fact]
		public void register_new_partition_state_stream() {
			var writes =
				_writeEventHandler.HandledMessages.Where(v => v.EventStreamId == "$projections-projection-partitions")
					.ToArray();
			Assert.Equal(1, writes.Length);
			var write = writes[0];

			Assert.Equal(1, write.Events.Length);

			var @event = write.Events[0];

			Assert.Equal("account-01", Helper.UTF8NoBom.GetString(@event.Data));
		}
	}
}
