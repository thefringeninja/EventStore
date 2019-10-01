﻿using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Utils;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader {
	public class when_receiving_state_report_response : specification_with_projection_manager_response_reader_started {
		private Guid _projectionId;
		private Guid _correlationId;
		private string _partition;
		private string _state;
		private CheckpointTag _position;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			_correlationId = Guid.NewGuid();
			_partition = "partition";
			_state = "{\"state\":1}";
			_position = CheckpointTag.FromStreamPosition(1, "stream", 2);

			yield return
				CreateWriteEvent(
					"$projections-$master",
					"$state",
					@"{
                        ""id"":""" + _projectionId.ToString("N") + @""",
                        ""correlationId"":""" + _correlationId.ToString("N") + @""",
                        ""partition"":""" + _partition + @""",
                        ""state"":" + _state.ToJson() + @",
                        ""position"":" + _position.ToJsonString() + @",
                    }",
					null,
					true);
		}

		[Fact]
		public void publishes_state_report_message() {
			var response =
				HandledMessages.OfType<CoreProjectionStatusMessage.StateReport>().LastOrDefault();
			Assert.NotNull(response);
			Assert.Equal(_projectionId, response.ProjectionId);
			Assert.Equal(_correlationId, response.CorrelationId);
			Assert.Equal(_partition, response.Partition);
			Assert.Equal(_state, response.State);
			Assert.Equal(_position, response.Position);
		}
	}
}
