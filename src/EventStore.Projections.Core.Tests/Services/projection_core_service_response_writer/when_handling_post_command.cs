using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using EventStore.Projections.Core.Services;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer {
	public class when_handling_post_command : specification_with_projection_core_service_response_writer {
		private string _name;
		private ProjectionManagementMessage.RunAs _runAs;
		private ProjectionMode _mode;
		private string _handlerType;
		private string _query;
		private bool _enabled;
		private bool _checkpointsEnabled;
		private bool _emitEnabled;
		private bool _enableRunAs;
		private bool _trackEmittedStreams;

		protected override void Given() {
			_name = "name";
			_runAs = ProjectionManagementMessage.RunAs.System;
			_mode = ProjectionMode.Continuous;
			_handlerType = "JS";
			_query = "fromAll();";
			_enabled = true;
			_checkpointsEnabled = true;
			_emitEnabled = true;
			_enableRunAs = true;
			_trackEmittedStreams = true;
		}

		protected override void When() {
			_sut.Handle(
				new ProjectionManagementMessage.Command.Post(
					new NoopEnvelope(),
					_mode,
					_name,
					_runAs,
					_handlerType,
					_query,
					_enabled,
					_checkpointsEnabled,
					_emitEnabled,
					_trackEmittedStreams,
					_enableRunAs));
		}

		[Fact]
		public void publishes_post_command() {
			var command = AssertParsedSingleCommand<PostCommand>("$post");
			Assert.Equal(_name, command.Name);
			Assert.Equal(_runAs, (ProjectionManagementMessage.RunAs)command.RunAs);
			Assert.Equal(_mode, command.Mode);
			Assert.Equal(_handlerType, command.HandlerType);
			Assert.Equal(_query, command.Query);
			Assert.Equal(_enabled, command.Enabled);
			Assert.Equal(_checkpointsEnabled, command.CheckpointsEnabled);
			Assert.Equal(_emitEnabled, command.EmitEnabled);
			Assert.Equal(_trackEmittedStreams, command.TrackEmittedStreams);
			Assert.Equal(_enableRunAs, command.EnableRunAs);
		}
	}
}
