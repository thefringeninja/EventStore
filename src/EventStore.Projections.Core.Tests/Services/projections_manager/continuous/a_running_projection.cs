using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.continuous {
	public class a_running_projection {
		public abstract class Base : a_new_posted_projection.Base {
			protected Guid _reader;

			protected override void Given() {
				base.Given();
			}

			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;
				var readerAssignedMessage =
					Consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.ReaderAssignedReader>()
						.LastOrDefault();
				if (_projectionEnabled) {
					Assert.NotNull(readerAssignedMessage);
					_reader = readerAssignedMessage.ReaderId;

					yield return
						(ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
							_reader, new TFPos(100, 50), new TFPos(100, 50), "stream", 1, "stream", 1, false,
							Guid.NewGuid(), "type", false, new byte[0], new byte[0], 100, 33.3f));
				} else
					_reader = Guid.Empty;
			}
		}

		public class when_stopping : Base {
			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;

				yield return
					(new ProjectionManagementMessage.Command.Disable(
						new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System));
				for (var i = 0; i < 50; i++) {
					yield return
						(ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
							_reader, new TFPos(100 * i + 200, 150), new TFPos(100 * i + 200, 150), "stream", 1 + i + 1,
							"stream", 1 + i + 1, false, Guid.NewGuid(), "type", false, new byte[0], new byte[0],
							100 * i + 200, 33.3f));
				}
			}

			[Fact(Skip = "actually in unsubscribes...")]
			public void pause_message_is_published() {
			}

			[Fact(Skip = "actually in unsubscribes...")]
			public void unsubscribe_message_is_published() {
			}


			[Fact]
			public void the_projection_status_becomes_stopped_disabled() {
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

		public class when_handling_event : Base {
			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;
				yield return
					(ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
						_reader, new TFPos(200, 150), new TFPos(200, 150), "stream", 2, "stream", 2, false,
						Guid.NewGuid(), "type", false, new byte[0], new byte[0], 100, 33.3f));
			}

			[Fact]
			public void the_projection_status_remains_running_enabled() {
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

		public class when_resetting : Base {
			protected override void Given() {
				base.Given();
				_projectionEnabled = false;
			}

			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;

				yield return
					(new ProjectionManagementMessage.Command.Reset(
						new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System));
			}

			[Fact]
			public void the_projection_epoch_changes() {
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
					1,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.Epoch);
				Assert.Equal(
					1,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.Version);
			}

			[Fact]
			public void the_projection_status_is_enabled_running() {
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

		public class when_resetting_and_starting : Base {
			protected override void Given() {
				base.Given();
				_projectionEnabled = false;
			}

			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;
				yield return
					(new ProjectionManagementMessage.Command.Reset(
						new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System));
				yield return
					(new ProjectionManagementMessage.Command.Enable(
						new PublishEnvelope(_bus), _projectionName, ProjectionManagementMessage.RunAs.System));
				yield return
					(ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
						_reader, new TFPos(100, 150), new TFPos(100, 150), "stream", 1 + 1, "stream", 1 + 1, false,
						Guid.NewGuid(), "type", false, new byte[0], new byte[0], 200, 33.3f));
			}

			[Fact]
			public void the_projection_epoch_changes() {
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
					1,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.Epoch);
				Assert.Equal(
					2,
					Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>()
						.Single()
						.Projections.Single()
						.Version);
			}

			[Fact]
			public void the_projection_status_is_enabled_running() {
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
