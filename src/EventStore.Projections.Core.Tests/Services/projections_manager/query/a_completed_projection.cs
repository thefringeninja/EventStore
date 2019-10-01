using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Management;
using Xunit;
using EventStore.Projections.Core.Services.Processing;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.query {
	namespace a_completed_projection {
		public abstract class Base : a_new_posted_projection.Base {
			protected Guid _reader;

			protected override void Given() {
				base.Given();
				AllWritesToSucceed(ProjectionNamesBuilder.ProjectionsStreamPrefix + _projectionName + "-result");
				AllWritesToSucceed("$$$projections-" + _projectionName + "-result");
				NoOtherStreams();
			}

			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;

				var readerAssignedMessage =
					Consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.ReaderAssignedReader>()
						.LastOrDefault();
				Assert.NotNull(readerAssignedMessage);
				_reader = readerAssignedMessage.ReaderId;

				yield return (
					ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
						_reader, new TFPos(100, 50), new TFPos(100, 50), "stream", 1, "stream", 1, false,
						Guid.NewGuid(), "type",
						false, new byte[0], new byte[0], 100, 33.3f));
				yield return (new ReaderSubscriptionMessage.EventReaderEof(_reader));
			}
		}

		public class when_stopping : Base {
			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;

				yield return
					(new ProjectionManagementMessage.Command.Disable(
						new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
			}

			[Fact]
			public void the_projection_status_becomes_completed_disabled() {
				_manager.Handle(
					new ProjectionManagementMessage.Command.GetStatistics(
						new PublishEnvelope(_bus), null, _projectionName, false));

				Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count());
				Assert.Equal(
					1,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Length);
				Assert.Equal(
					_projectionName,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.Name);
				Assert.Equal(
					ManagedProjectionState.Stopped,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.MasterStatus);
				Assert.Equal(
					false,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.Enabled);
			}
		}

		public class when_starting : Base {
			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;
				yield return
					(new ProjectionManagementMessage.Command.Enable(
						new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
			}

			[Fact]
			public void the_projection_status_becomes_running_enabled() {
				_manager.Handle(
					new ProjectionManagementMessage.Command.GetStatistics(
						new PublishEnvelope(_bus), null, _projectionName, false));

				Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().Count());
				Assert.Equal(
					1,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Length);
				Assert.Equal(
					_projectionName,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.Name);
				Assert.Equal(
					ManagedProjectionState.Running,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.MasterStatus);
				Assert.Equal(
					true,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.Enabled);
			}
		}
	}
}
