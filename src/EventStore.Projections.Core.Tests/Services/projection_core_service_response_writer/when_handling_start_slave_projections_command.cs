using System;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using EventStore.Projections.Core.Services;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer {
	public class
		when_handling_start_slave_projections_command : specification_with_projection_core_service_response_writer {
		private string _name;
		private ProjectionManagementMessage.RunAs _runAs;
		private SlaveProjectionDefinitions _definition;
		private Guid _masterWorkerId;
		private Guid _masterCorrelationId;

		protected override void Given() {
			_name = "name";
			_runAs = ProjectionManagementMessage.RunAs.System;
			_masterCorrelationId = Guid.NewGuid();
			_masterWorkerId = Guid.NewGuid();
			_definition =
				new SlaveProjectionDefinitions(
					new SlaveProjectionDefinitions.Definition(
						"sl1",
						"JS",
						"fromAll()",
						SlaveProjectionDefinitions.SlaveProjectionRequestedNumber.OnePerThread,
						ProjectionMode.Transient,
						true,
						true,
						true,
						true,
						ProjectionManagementMessage.RunAs.System));
		}

		protected override void When() {
			_sut.Handle(
				new ProjectionManagementMessage.Command.StartSlaveProjections(
					new NoopEnvelope(),
					_runAs,
					_name,
					_definition,
					_masterWorkerId,
					_masterCorrelationId));
		}

		[Fact]
		public void publishes_start_slave_projections_command() {
			var command = AssertParsedSingleCommand<StartSlaveProjectionsCommand>("$start-slave-projections");
			Assert.Equal(_name, command.Name);
			Assert.Equal(_runAs, (ProjectionManagementMessage.RunAs)command.RunAs);
			Assert.Equal(_masterCorrelationId.ToString("N"), command.MasterCorrelationId);
			Assert.Equal(_masterWorkerId.ToString("N"), command.MasterWorkerId);
			Assert.NotNull(command.SlaveProjections);
			Assert.NotNull(command.SlaveProjections.Definitions);
			Assert.Equal(1, command.SlaveProjections.Definitions.Length);
			var definition = _definition.Definitions[0];
			var received = command.SlaveProjections.Definitions[0];
			Assert.Equal(definition.CheckpointsEnabled, received.CheckpointsEnabled);
			Assert.Equal(definition.EmitEnabled, received.EmitEnabled);
			Assert.Equal(definition.EnableRunAs, received.EnableRunAs);
			Assert.Equal(definition.HandlerType, received.HandlerType);
			Assert.Equal(definition.Mode, received.Mode);
			Assert.Equal(definition.Name, received.Name);
			Assert.Equal(definition.Query, received.Query);
			Assert.Equal(definition.RequestedNumber, received.RequestedNumber);
			Assert.Equal(definition.RunAs1.Name, received.RunAs1.Name);
		}
	}
}
