using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer {
	public class when_handling_disable_command : specification_with_projection_core_service_response_writer {
		private string _name;
		private ProjectionManagementMessage.RunAs _runAs;

		protected override void Given() {
			_name = "name";
			_runAs = ProjectionManagementMessage.RunAs.System;
		}

		protected override void When() {
			_sut.Handle(new ProjectionManagementMessage.Command.Disable(new NoopEnvelope(), _name, _runAs));
		}

		[Fact]
		public void publishes_disable_command() {
			var command = AssertParsedSingleCommand<DisableCommand>("$disable");
			Assert.Equal(_name, command.Name);
			Assert.Equal(_runAs, (ProjectionManagementMessage.RunAs)command.RunAs);
		}
	}
}
