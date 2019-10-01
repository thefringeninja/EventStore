using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class when_the_onetime_projection_has_been_posted : TestFixtureWithProjectionCoreAndManagementServices {
		private string _projectionName;
		private string _projectionQuery;

		protected override void Given() {
			NoOtherStreams();
		}

		protected override IEnumerable<WhenStep> When() {
			_projectionQuery = @"fromAll(); on_any(function(){});log(1);";
			yield return (new SystemMessage.BecomeMaster(Guid.NewGuid()));
			yield return (new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now)));
			yield return (new SystemMessage.SystemCoreReady());
			yield return
				(new ProjectionManagementMessage.Command.Post(
					new PublishEnvelope(_bus), ProjectionManagementMessage.RunAs.Anonymous, _projectionQuery,
					enabled: true));
			_projectionName = Consumer.HandledMessages.OfType<ProjectionManagementMessage.Updated>().Single().Name;
		}

		[Fact, Trait("Category", "v8")]
		public void it_has_been_posted() {
			Assert.False(string.IsNullOrEmpty(_projectionName));
		}

		[Fact, Trait("Category", "v8")]
		public void it_cab_be_listed() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetStatistics(new PublishEnvelope(_bus), null, null, false));

			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count(
					v => v.Projections.Any(p => p.Name == _projectionName)));
		}

		[Fact, Trait("Category", "v8")]
		public void the_projection_status_can_be_retrieved() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetStatistics(
					new PublishEnvelope(_bus), null, _projectionName, false));

			Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count());
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections.Length);
			Assert.Equal(
				_projectionName,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections.Single()
					.Name);
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

		[Fact, Trait("Category", "v8")]
		public void the_projection_source_can_be_retrieved() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetQuery(
					new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
			Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().Count());
			var projectionQuery = Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>()
				.Single();
			Assert.Equal(_projectionName, projectionQuery.Name);
			Assert.Equal(_projectionQuery, projectionQuery.Query);
		}
	}
}
