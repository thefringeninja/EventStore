using System;
using EventStore.Core.Authentication;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Commands;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.command_writer {
	public class when_handling_create_and_prepare_message : specification_with_projection_manager_command_writer {
		private Guid _projectionId;
		private Guid _workerId;
		private ProjectionConfig _config;
		private ProjectionVersion _projectionVersion;
		private string _projectionName;
		private string _handlerType;
		private string _query;

		protected override void Given() {
			_projectionId = Guid.NewGuid();
			_workerId = Guid.NewGuid();
			_projectionName = "projection";
			_handlerType = "JS";
			_query = "from()";

			_config = new ProjectionConfig(
				new OpenGenericPrincipal("user", "a", "b"),
				1000,
				100000,
				2000,
				200,
				true,
				true,
				true,
				true,
				true,
				true,
				10000,
				1);
		}

		protected override void When() {
			_projectionVersion = new ProjectionVersion(1, 2, 3);
			_sut.Handle(
				new CoreProjectionManagementMessage.CreateAndPrepare(
					_projectionId,
					_workerId,
					_projectionName,
					_projectionVersion,
					_config,
					_handlerType,
					_query));
		}

		[Fact]
		public void publishes_create_and_prepare_command() {
			var command =
				AssertParsedSingleCommand<CreateAndPrepareCommand>(
					"$create-and-prepare",
					_workerId);
			Assert.Equal(_projectionId.ToString("N"), command.Id);
			Assert.Equal(_handlerType, command.HandlerType);
			Assert.Equal(_projectionName, command.Name);
			Assert.Equal(_query, command.Query);
			Assert.Equal(
				(PersistedProjectionVersion)_projectionVersion,
				command.Version);
			Assert.Equal(_config.CheckpointHandledThreshold, command.Config.CheckpointHandledThreshold);
			Assert.Equal(_config.CheckpointUnhandledBytesThreshold,
				command.Config.CheckpointUnhandledBytesThreshold);
			Assert.Equal(_config.CheckpointsEnabled, command.Config.CheckpointsEnabled);
			Assert.Equal(_config.CreateTempStreams, command.Config.CreateTempStreams);
			Assert.Equal(_config.EmitEventEnabled, command.Config.EmitEventEnabled);
			Assert.Equal(_config.IsSlaveProjection, command.Config.IsSlaveProjection);
			Assert.Equal(_config.MaxWriteBatchLength, command.Config.MaxWriteBatchLength);
			Assert.Equal(_config.PendingEventsThreshold, command.Config.PendingEventsThreshold);
			Assert.Equal(_config.MaximumAllowedWritesInFlight, command.Config.MaximumAllowedWritesInFlight);
			Assert.Equal(_config.RunAs.Identity.Name, command.Config.RunAs);
			Assert.Equal(_config.StopOnEof, command.Config.StopOnEof);
		}
	}
}
