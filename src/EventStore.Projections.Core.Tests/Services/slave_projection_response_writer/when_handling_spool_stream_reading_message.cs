using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Commands;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.slave_projection_response_writer {
	public class when_handling_spool_stream_reading_message : specification_with_slave_projection_response_writer {
		private Guid _workerId;
		private Guid _subscriptionId;

		protected override void Given() {
			_workerId = Guid.NewGuid();
			_subscriptionId = Guid.NewGuid();
		}

		protected override void When() {
			_sut.Handle(
				new ReaderSubscriptionManagement.SpoolStreamReading(_workerId, _subscriptionId, "stream1", 100,
					1000000));
		}

		[Fact]
		public void publishes_partition_measured_response() {
			var body =
				AssertParsedSingleResponse<SpoolStreamReadingCommand>(
					"$spool-stream-reading",
					_workerId);

			Assert.Equal(_subscriptionId.ToString("N"), body.SubscriptionId);
			Assert.Equal("stream1", body.StreamId);
			Assert.Equal(100, body.CatalogSequenceNumber);
			Assert.Equal(1000000, body.LimitingCommitPosition);
		}
	}
}
