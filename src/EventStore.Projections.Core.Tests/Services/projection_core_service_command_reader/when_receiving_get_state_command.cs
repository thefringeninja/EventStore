using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_command_reader {
	public class when_receiving_get_state_command : specification_with_projection_core_service_command_reader_started {
		private Guid _projectionId;
		private Guid _correlationId;
		private string _partition;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			_correlationId = Guid.NewGuid();
			_partition = "partition";
			yield return
				CreateWriteEvent(
					"$projections-$" + _serviceId,
					"$get-state",
					@"{
                        ""id"":""" + _projectionId.ToString("N") + @""",
                        ""correlationId"":""" + _correlationId.ToString("N") + @""",
                        ""partition"":""" + _partition + @""",
                    }",
					null,
					true);
		}

		[Fact]
		public void publishes_projection_kill_message() {
			var command = HandledMessages.OfType<CoreProjectionManagementMessage.GetState>().LastOrDefault();
			Assert.NotNull(command);
			Assert.Equal(_projectionId, command.ProjectionId);
			Assert.Equal(_correlationId, command.CorrelationId);
			Assert.Equal(_partition, command.Partition);
		}
	}
}
