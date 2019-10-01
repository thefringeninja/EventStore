using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer {
	public class
		when_handling_set_runas_command : specification_with_projection_core_service_response_writer {
		private string _name;
		private ProjectionManagementMessage.RunAs _runAs;
		private ProjectionManagementMessage.Command.SetRunAs.SetRemove _setRemove;

		protected override void Given() {
			_name = "name";
			_runAs = ProjectionManagementMessage.RunAs.System;
			_setRemove = ProjectionManagementMessage.Command.SetRunAs.SetRemove.Remove;
		}

		protected override void When() {
			_sut.Handle(
				new ProjectionManagementMessage.Command.SetRunAs(new NoopEnvelope(), _name, _runAs, _setRemove));
		}

		[Fact]
		public void publishes_set_runas_command() {
			var command = AssertParsedSingleCommand<SetRunAsCommand>("$set-runas");
			Assert.Equal(_name, command.Name);
			Assert.Equal(_runAs, (ProjectionManagementMessage.RunAs)command.RunAs);
			Assert.Equal(_setRemove, command.SetRemove);
		}
	}
}
