using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class when_updating_a_persistent_projection_emit_enabled_option :
		TestFixtureWithProjectionCoreAndManagementServices {
		protected override void Given() {
			NoStream("$projections-test-projection");
			NoStream("$projections-test-projection-result");
			NoStream("$projections-test-projection-order");
			AllWritesToSucceed("$projections-test-projection-order");
			NoStream("$projections-test-projection-checkpoint");
			AllWritesSucceed();
		}

		private string _projectionName;
		private string _source;

		protected override IEnumerable<WhenStep> When() {
			_projectionName = "test-projection";
			_source = @"fromAll(); on_any(function(){});log(1);";
			yield return (new SystemMessage.BecomeMaster(Guid.NewGuid()));
			yield return (new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now)));
			yield return (new SystemMessage.SystemCoreReady());
			yield return
				(new ProjectionManagementMessage.Command.Post(
					new PublishEnvelope(_bus), ProjectionMode.Continuous, _projectionName,
					ProjectionManagementMessage.RunAs.System, "JS", _source, enabled: true, checkpointsEnabled: true,
					emitEnabled: true, trackEmittedStreams: true));
			// when
			yield return
				(new ProjectionManagementMessage.Command.UpdateQuery(
					new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System, "JS",
					_source, emitEnabled: false));
		}

		[Fact, Trait("Category", "v8")]
		public void emit_enabled_options_remains_unchanged() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetQuery(
					new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
			Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Count());
			var projectionQuery =
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Single();
			Assert.Equal(_projectionName, projectionQuery.Name);
			Assert.False(projectionQuery.EmitEnabled);
		}
	}
}
