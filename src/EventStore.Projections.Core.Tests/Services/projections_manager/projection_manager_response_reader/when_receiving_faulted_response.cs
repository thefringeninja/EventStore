using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader {
	public class when_receiving_faulted_response : specification_with_projection_manager_response_reader_started {
		private Guid _projectionId;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			yield return
				CreateWriteEvent(
					"$projections-$master",
					"$faulted",
					@"{
                        ""id"":""" + _projectionId.ToString("N") + @""",
                        ""faultedReason"":""reason""
                    }",
					null,
					true);
		}

		[Fact]
		public void publishes_faulted_message() {
			var response =
				HandledMessages.OfType<CoreProjectionStatusMessage.Faulted>().LastOrDefault();
			Assert.NotNull(response);
			Assert.Equal(_projectionId, response.ProjectionId);
			Assert.Equal("reason", response.FaultedReason);
		}
	}
}
