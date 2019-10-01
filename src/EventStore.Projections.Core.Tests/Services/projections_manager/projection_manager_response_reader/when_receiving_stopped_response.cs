using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader {
	public class when_receiving_stopped_response : specification_with_projection_manager_response_reader_started {
		private Guid _projectionId;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			yield return
				CreateWriteEvent(
					"$projections-$master",
					"$stopped",
					@"{
                        ""id"":""" + _projectionId.ToString("N") + @""",
                    }",
					null,
					true);
		}

		[Fact]
		public void publishes_stopped_message() {
			var response =
				HandledMessages.OfType<CoreProjectionStatusMessage.Stopped>().LastOrDefault();
			Assert.NotNull(response);
			Assert.Equal(_projectionId, response.ProjectionId);
		}
	}
}
