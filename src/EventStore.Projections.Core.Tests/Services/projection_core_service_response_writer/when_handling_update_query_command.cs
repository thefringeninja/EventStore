using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer {
	public class
		when_handling_update_query_command : specification_with_projection_core_service_response_writer {
		private string _name;
		private ProjectionManagementMessage.RunAs _runAs;
		private string _handlerType;
		private string _query;
		private bool? _emitEnabled;

		protected override void Given() {
			_name = "name";
			_runAs = ProjectionManagementMessage.RunAs.System;
			_handlerType = "JS";
			_query = "fromAll()";
			_emitEnabled = true;
		}

		protected override void When() {
			_sut.Handle(
				new ProjectionManagementMessage.Command.UpdateQuery(
					new NoopEnvelope(),
					_name,
					_runAs,
					_handlerType,
					_query,
					_emitEnabled));
		}

		[Fact]
		public void publishes_update_query_command() {
			var command = AssertParsedSingleCommand<UpdateQueryCommand>("$update-query");
			Assert.Equal(_name, command.Name);
			Assert.Equal(_runAs, (ProjectionManagementMessage.RunAs)command.RunAs);
			Assert.Equal(_handlerType, command.HandlerType);
			Assert.Equal(_query, command.Query);
			Assert.Equal(_emitEnabled, command.EmitEnabled);
		}
	}
}
