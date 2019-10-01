using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class when_recreating_a_deleted_projection : TestFixtureWithProjectionCoreAndManagementServices {
		private string _projectionName;

		protected override void Given() {
			_projectionName = "test-projection";
			AllWritesSucceed();
			NoOtherStreams();
		}

		protected override IEnumerable<WhenStep> When() {
			yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
			yield return new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now));
			yield return new SystemMessage.SystemCoreReady();
			yield return
				new ProjectionManagementMessage.Command.Post(
					new PublishEnvelope(_bus), ProjectionMode.Continuous, _projectionName,
					ProjectionManagementMessage.RunAs.System, "JS", @"fromAll().when({$any:function(s,e){return s;}});",
					enabled: true, checkpointsEnabled: true, emitEnabled: true, trackEmittedStreams: true);
			yield return
				new ProjectionManagementMessage.Command.Disable(
					new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System);
			yield return
				new ProjectionManagementMessage.Command.Delete(
					new PublishEnvelope(_bus), _projectionName,
					ProjectionManagementMessage.RunAs.System, true, true, false);
			yield return
				new ProjectionManagementMessage.Command.Post(
					new PublishEnvelope(_bus), ProjectionMode.Continuous, _projectionName,
					ProjectionManagementMessage.RunAs.System, "JS", @"fromAll().when({$any:function(s,e){return s;}});",
					enabled: true, checkpointsEnabled: true, emitEnabled: true, trackEmittedStreams: true);
		}

		[Fact, Trait("Category", "v8")]
		public void a_projection_created_event_should_be_written() {
			Assert.Equal(
				ProjectionEventTypes.ProjectionCreated,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().First().Events[0].EventType);
			Assert.Equal(
				_projectionName,
				Helper.UTF8NoBom.GetString(Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().First()
					.Events[0].Data));
		}

		[Fact, Trait("Category", "v8")]
		public void it_can_be_listed() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetStatistics(new PublishEnvelope(_bus), null, null, false));

			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count(
					v => v.Projections.Any(p => p.Name == _projectionName)));
		}
	}
}
