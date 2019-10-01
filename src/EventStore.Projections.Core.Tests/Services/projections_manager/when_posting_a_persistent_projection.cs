using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class when_posting_a_persistent_projection : TestFixtureWithProjectionCoreAndManagementServices {
		private string _projectionName;

		protected override void Given() {
			_projectionName = "test-projection";
			AllWritesQueueUp();
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
			OneWriteCompletes();
		}

		[Fact, Trait("Category", "v8")]
		public void the_projection_status_is_writing() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetStatistics(new PublishEnvelope(_bus), null, _projectionName,
					true));
			Assert.Equal(
				ManagedProjectionState.Prepared,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Single().Projections[0]
					.MasterStatus);
		}

		[Fact, Trait("Category", "v8")]
		public void a_projection_created_event_is_written() {
			Assert.Equal(
				ProjectionEventTypes.ProjectionCreated,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().First().Events[0].EventType);
			Assert.Equal(
				_projectionName,
				Helper.UTF8NoBom.GetString(Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().First()
					.Events[0].Data));
		}

		[Fact, Trait("Category", "v8")]
		public void a_projection_updated_message_is_not_published() {
			// not published until all writes complete
			Assert.Equal(0, Consumer.HandledMessages.OfType<ProjectionManagementMessage.Updated>().Count());
		}
	}
}
