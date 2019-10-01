﻿using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Commands;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.command_writer {
	public class when_handling_start_message : specification_with_projection_manager_command_writer {
		private Guid _projectionId;
		private Guid _workerId;

		protected override void Given() {
			_projectionId = Guid.NewGuid();
			_workerId = Guid.NewGuid();
		}

		protected override void When() {
			_sut.Handle(new CoreProjectionManagementMessage.Start(_projectionId, _workerId));
		}

		[Fact]
		public void publishes_start_command() {
			var command = AssertParsedSingleCommand<StartCommand>("$start", _workerId);
			Assert.Equal(_projectionId.ToString("N"), command.Id);
		}
	}
}
