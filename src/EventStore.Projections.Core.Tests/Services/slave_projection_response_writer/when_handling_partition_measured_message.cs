using System;
using EventStore.Projections.Core.Messages.ParallelQueryProcessingMessages;
using EventStore.Projections.Core.Messages.Persisted.Responses.Slave;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.slave_projection_response_writer {
	public class when_handling_partition_measured_message : specification_with_slave_projection_response_writer {
		private Guid _workerId;
		private Guid _masterProjectionId;
		private Guid _subscriptionId;
		private string _partition;
		private int _size;

		protected override void Given() {
			_workerId = Guid.NewGuid();
			_subscriptionId = Guid.NewGuid();
			_masterProjectionId = Guid.NewGuid();
			_partition = "partition";
			_size = 123;
		}

		protected override void When() {
			_sut.Handle(new PartitionMeasuredOutput(_workerId, _masterProjectionId, _subscriptionId, _partition,
				_size));
		}

		[Fact]
		public void publishes_partition_measured_response() {
			var body = AssertParsedSingleResponse<PartitionMeasuredResponse>("$measured", _masterProjectionId);
			Assert.Equal(_subscriptionId.ToString("N"), body.SubscriptionId);
			Assert.Equal(_partition, body.Partition);
			Assert.Equal(_size, body.Size);
		}
	}
}
