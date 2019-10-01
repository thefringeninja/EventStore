using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_command_reader {
	public class when_receiving_dispose_command : specification_with_projection_core_service_command_reader_started {
		private Guid _projectionId;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			yield return
				CreateWriteEvent(
					"$projections-$" + _serviceId,
					"$dispose",
					"{\"id\":\"" + _projectionId.ToString("N") + "\"}",
					null,
					true);
		}

		[Fact]
		public void publishes_projection_dispose_message() {
			var dispose = HandledMessages.OfType<CoreProjectionManagementMessage.Dispose>().LastOrDefault();
			Assert.NotNull(dispose);
			Assert.Equal(_projectionId, dispose.ProjectionId);
		}
	}
}
