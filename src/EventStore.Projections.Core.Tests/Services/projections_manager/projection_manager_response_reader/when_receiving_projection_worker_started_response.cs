using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader {
	public class when_receiving_projection_worker_started_response
		: specification_with_projection_manager_response_reader_started {
		private Guid _workerId;

		protected override IEnumerable<WhenStep> When() {
			_workerId = Guid.NewGuid();
			yield return CreateWriteEvent("$projections-$master", "$projection-worker-started", @"{
                        ""id"":""" + _workerId.ToString("N") + @""",
                    }", null, true);
		}

		[Fact]
		public void publishes_projection_worker_started_message() {
			var response = HandledMessages.OfType<CoreProjectionStatusMessage.ProjectionWorkerStarted>()
				.LastOrDefault();
			Assert.NotNull(response);
			Assert.Equal(_workerId, response.WorkerId);
		}
	}
}
