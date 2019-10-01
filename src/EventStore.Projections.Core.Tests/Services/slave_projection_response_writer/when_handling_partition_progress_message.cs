using System;
using EventStore.Projections.Core.Messages.ParallelQueryProcessingMessages;
using EventStore.Projections.Core.Messages.Persisted.Responses.Slave;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.slave_projection_response_writer {
	public class when_handling_partition_progress_message : specification_with_slave_projection_response_writer {
		private Guid _workerId;
		private Guid _masterProjectionId;
		private Guid _subscriptionId;
		private float _progress;

		protected override void Given() {
			_workerId = Guid.NewGuid();
			_masterProjectionId = Guid.NewGuid();
			_subscriptionId = Guid.NewGuid();
			_progress = 123.4f;
		}

		protected override void When() {
			_sut.Handle(
				new PartitionProcessingProgressOutput(_workerId, _masterProjectionId, _subscriptionId, _progress));
		}

		[Fact]
		public void publishes_partition_processing_progress_response() {
			var body = AssertParsedSingleResponse<PartitionProcessingProgressResponse>("$progress",
				_masterProjectionId);
			Assert.Equal(_subscriptionId.ToString("N"), body.SubscriptionId);
			Assert.Equal(_progress, body.Progress);
		}
	}
}
