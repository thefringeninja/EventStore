using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer {
	public class when_handling_slave_projection_reader_assigned_message :
		specification_with_projection_core_service_response_writer {
		private Guid _projectionId;
		private Guid _subscriptionId;

		protected override void Given() {
			_projectionId = Guid.NewGuid();
			_subscriptionId = Guid.NewGuid();
		}

		protected override void When() {
			_sut.Handle(
				new CoreProjectionManagementMessage.SlaveProjectionReaderAssigned(
					_projectionId,
					_subscriptionId));
		}

		[Fact]
		public void publishes_slave_projection_reader_assigned_response() {
			var command = AssertParsedSingleCommand<SlaveProjectionReaderAssigned>("$slave-projection-reader-assigned");
			Assert.Equal(_projectionId.ToString("N"), command.Id);
			Assert.Equal(_subscriptionId.ToString("N"), command.SubscriptionId);
		}
	}
}
