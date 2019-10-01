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
	public class
		when_deleting_a_persistent_projection_and_keep_emitted_streams_stream :
			TestFixtureWithProjectionCoreAndManagementServices {
		private string _projectionName;
		private const string _projectionEmittedStreamsStream = "$projections-test-projection-emittedstreams";

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
					ProjectionManagementMessage.RunAs.System, false, false, false);
		}

		[Fact, Trait("Category", "v8")]
		public void a_projection_deleted_event_is_written() {
			Assert.Equal(
				true,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Any(x =>
					x.Events[0].EventType == ProjectionEventTypes.ProjectionDeleted &&
					Helper.UTF8NoBom.GetString(x.Events[0].Data) == _projectionName));
		}

		[Fact, Trait("Category", "v8")]
		public void should_not_have_attempted_to_delete_the_emitted_streams_stream() {
			Assert.False(
				Consumer.HandledMessages.OfType<ClientMessage.DeleteStream>()
					.Any(x => x.EventStreamId == _projectionEmittedStreamsStream));
		}
	}
}
