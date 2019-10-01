using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.query_by_stream {
	public class when_handling_multiple_empty_streams : specification_with_from_catalog_query {
		protected override void When() {
			//projection subscribes here
			_eventId = Guid.NewGuid();
			Consumer.HandledMessages.Clear();
			_bus.Publish(
				new EventReaderSubscriptionMessage.PartitionEofReached(
					_subscriptionId, CheckpointTag.FromByStreamPosition(0, "catalog", 0, null, -1, long.MinValue),
					"partition1", 0));
			_bus.Publish(
				new EventReaderSubscriptionMessage.PartitionEofReached(
					_subscriptionId, CheckpointTag.FromByStreamPosition(0, "catalog", 1, null, -1, long.MinValue),
					"partition2", 1));
			_bus.Publish(
				new EventReaderSubscriptionMessage.EofReached(
					_subscriptionId, CheckpointTag.FromByStreamPosition(0, "catalog", 1, null, -1, long.MinValue), 2));
		}

		[Fact]
		public void does_not_write_empty_state_for_each_partition() {
			Assert.Equal(0, _writeEventHandler.HandledMessages.OfEventType("Result").Count);
			Assert.Equal(0, _writeEventHandler.HandledMessages.OfEventType("ResultRemoved").Count);
		}
	}
}
