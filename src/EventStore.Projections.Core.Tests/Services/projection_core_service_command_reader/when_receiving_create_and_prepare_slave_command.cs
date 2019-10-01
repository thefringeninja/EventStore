using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_command_reader {
	public class
		when_receiving_create_and_prepare_slave_command :
			specification_with_projection_core_service_command_reader_started {
		private const string Query = @"fromStream('$user-admin').outputState()";
		private Guid _projectionId;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			yield return
				CreateWriteEvent(
					"$projections-$" + _serviceId,
					"$create-and-prepare-slave",
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
                             ""stopOnEof"":true,
                             ""isSlaveProjection"":true,
                         },
                         ""version"":{},
                         ""handlerType"":""JS"",
                         ""query"":""" + Query + @""",
                         ""name"":""test"",
                         ""masterWorkerId"":""2251f64ac0a24c599414d7fe2ada13b1"",
                         ""masterCoreProjectionId"":""85032c4bc58546b4be116f5c4b305bb6"",
                    }",
					null,
					true);
		}

		[Fact]
		public void publishes_projection_create_prepapre_slave_message() {
			var createPrepareSlave =
				HandledMessages.OfType<CoreProjectionManagementMessage.CreateAndPrepareSlave>().LastOrDefault();
			Assert.NotNull(createPrepareSlave);
			Assert.Equal(_projectionId, createPrepareSlave.ProjectionId);
			Assert.Equal("JS", createPrepareSlave.HandlerType);
			Assert.Equal(Query, createPrepareSlave.Query);
			Assert.Equal("test", createPrepareSlave.Name);
			Assert.NotNull(createPrepareSlave.Config);
			Assert.Equal("user", createPrepareSlave.Config.RunAs.Identity.Name);
			Assert.True(createPrepareSlave.Config.RunAs.IsInRole("b"));
			Assert.Equal(1000, createPrepareSlave.Config.CheckpointHandledThreshold);
			Assert.Equal(10000, createPrepareSlave.Config.CheckpointUnhandledBytesThreshold);
			Assert.Equal(5000, createPrepareSlave.Config.PendingEventsThreshold);
			Assert.Equal(100, createPrepareSlave.Config.MaxWriteBatchLength);
			Assert.Equal(1, createPrepareSlave.Config.MaximumAllowedWritesInFlight);
			Assert.Equal(true, createPrepareSlave.Config.EmitEventEnabled);
			Assert.Equal(true, createPrepareSlave.Config.CheckpointsEnabled);
			Assert.Equal(true, createPrepareSlave.Config.CreateTempStreams);
			Assert.Equal(true, createPrepareSlave.Config.StopOnEof);
			Assert.Equal(true, createPrepareSlave.Config.IsSlaveProjection);
		}
	}
}
