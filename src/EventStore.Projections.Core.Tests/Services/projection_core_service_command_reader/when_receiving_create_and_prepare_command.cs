using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_command_reader {
	public class
		when_receiving_create_and_prepare_command : specification_with_projection_core_service_command_reader_started {
		private const string Query = @"fromStream('$user-admin').outputState()";
		private Guid _projectionId;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			yield return
				CreateWriteEvent(
					"$projections-$" + _serviceId,
					"$create-and-prepare",
					@"{
                        ""id"":""" + _projectionId.ToString("N") + @""",
                          ""config"":{
                             ""runAs"":""user"",
                             ""runAsRoles"":[""a"",""b""],
                             ""checkpointHandledThreshold"":1000,
                             ""checkpointUnhandledBytesThreshold"":10000,
                             ""pendingEventsThreshold"":5000, 
                             ""maxWriteBatchLength"":100,
                             ""maximumAllowedWritesInFlight"":1,
                             ""emitEventEnabled"":true,
                             ""checkpointsEnabled"":true,
                             ""createTempStreams"":true,
                             ""stopOnEof"":false,
                             ""isSlaveProjection"":false,
                         },
                         ""version"":{},
                         ""handlerType"":""JS"",
                         ""query"":""" + Query + @""",
                         ""name"":""test""
                    }",
					null,
					true);
		}

		[Fact]
		public void publishes_projection_create_prepapre_message() {
			var createPrepare =
				HandledMessages.OfType<CoreProjectionManagementMessage.CreateAndPrepare>().LastOrDefault();
			Assert.NotNull(createPrepare);
			Assert.Equal(_projectionId, createPrepare.ProjectionId);
			Assert.Equal("JS", createPrepare.HandlerType);
			Assert.Equal(Query, createPrepare.Query);
			Assert.Equal("test", createPrepare.Name);
			Assert.NotNull(createPrepare.Config);
			Assert.Equal("user", createPrepare.Config.RunAs.Identity.Name);
			Assert.True(createPrepare.Config.RunAs.IsInRole("b"));
			Assert.Equal(1000, createPrepare.Config.CheckpointHandledThreshold);
			Assert.Equal(10000, createPrepare.Config.CheckpointUnhandledBytesThreshold);
			Assert.Equal(5000, createPrepare.Config.PendingEventsThreshold);
			Assert.Equal(100, createPrepare.Config.MaxWriteBatchLength);
			Assert.Equal(1, createPrepare.Config.MaximumAllowedWritesInFlight);
			Assert.Equal(true, createPrepare.Config.EmitEventEnabled);
			Assert.Equal(true, createPrepare.Config.CheckpointsEnabled);
			Assert.Equal(true, createPrepare.Config.CreateTempStreams);
			Assert.False(createPrepare.Config.StopOnEof);
			Assert.False(createPrepare.Config.IsSlaveProjection);
		}
	}
}
