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
	public class when_updating_an_onetime_projection_query_text : TestFixtureWithProjectionCoreAndManagementServices {
		private string _projectionName;
		private string _newProjectionSource;

		protected override void Given() {
			NoOtherStreams();
		}

		[Fact, Trait("Category", "v8")]
		public void the_projection_source_can_be_retrieved() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetQuery(
					new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
			Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Count());
			var projectionQuery =
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Single();
			Assert.Equal(_projectionName, projectionQuery.Name);
			Assert.Equal(_newProjectionSource, projectionQuery.Query);
		}

		[Fact, Trait("Category", "v8")]
		public void the_projection_status_is_still_running() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetStatistics(new PublishEnvelope(_bus), null, _projectionName,
					false));

			Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count());
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections.Length);
			Assert.Equal(
				_projectionName,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections.Single()
					.Name);
			Assert.Equal(
				ManagedProjectionState.Running,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections.Single()
					.MasterStatus);
		}

		[Fact, Trait("Category", "v8")]
		public void the_projection_state_can_be_retrieved() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetState(new PublishEnvelope(_bus), _projectionName, ""));
			Queue.Process();

			Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Count());
			Assert.Equal(
				_projectionName,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Single().Name);
			Assert.Equal(
				"", Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Single().State);
		}

		protected override IEnumerable<WhenStep> When() {
			_projectionName = "test-projection";
			yield return (new SystemMessage.BecomeMaster(Guid.NewGuid()));
			yield return (new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now)));
			yield return (new SystemMessage.SystemCoreReady());
			yield return
				(new ProjectionManagementMessage.Command.Post(
					new PublishEnvelope(_bus), ProjectionMode.Transient, _projectionName,
					ProjectionManagementMessage.RunAs.Anonymous, "JS", @"fromAll(); on_any(function(){});log(1);",
					enabled: true, checkpointsEnabled: false, emitEnabled: false, trackEmittedStreams: true));
			// when
			_newProjectionSource = @"fromAll(); on_any(function(){});log(2);";
			yield return
				(new ProjectionManagementMessage.Command.UpdateQuery(
					new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous, "JS",
					_newProjectionSource, emitEnabled: null));
		}
	}
}
