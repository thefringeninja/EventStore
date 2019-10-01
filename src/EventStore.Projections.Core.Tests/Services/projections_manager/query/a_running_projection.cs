using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.query {
	namespace a_running_projection {
		public abstract class Base : a_new_posted_projection.Base {
			protected Guid _reader;

			protected override void Given() {
				base.Given();
				AllWritesSucceed();
				NoOtherStreams();
			}

			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;
				var readerAssignedMessage =
					Consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.ReaderAssignedReader>()
						.LastOrDefault();
				Assert.NotNull(readerAssignedMessage);
				_reader = readerAssignedMessage.ReaderId;
				Consumer.HandledMessages.Clear();
				yield return
					(ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
						_reader, new TFPos(100, 50), new TFPos(100, 50), "stream", 1, "stream", 1, false,
						Guid.NewGuid(),
						"type", false, new byte[0], new byte[0], 100, 33.3f));
			}
		}

		public class when_handling_eof : Base {
			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;

				yield return (new ReaderSubscriptionMessage.EventReaderEof(_reader));
			}

			[Fact(Skip = "actually in unsubscribes...")]
			public void pause_message_is_published() {
			}


			[Fact]
			public void the_projection_status_becomes_completed_enabled() {
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
					ManagedProjectionState.Completed,
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

			[Fact]
			public void writes_result_stream() {
				List<EventRecord> resultsStream;
				Assert.True((_streams.TryGetValue("$projections-test-projection-result", out resultsStream)));
				Assert.Equal(1 + 1 /* $Eof*/, resultsStream.Count);
				Assert.Equal("{\"data\": 1}", Encoding.UTF8.GetString(resultsStream[0].Data));
			}

			[Fact]
			public void does_not_write_to_any_other_streams() {
				Assert.Empty(
					HandledMessages.OfType<ClientMessage.WriteEvents>()
						.Where(v => v.EventStreamId != "$projections-test-projection-result")
						.Where(v => v.EventStreamId != "$$$projections-test-projection-result")
						.Select(v => v.EventStreamId));
			}
		}

		public class when_handling_event : Base {
			protected override IEnumerable<WhenStep> When() {
				foreach (var m in base.When()) yield return m;
				yield return
					(ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
						_reader, new TFPos(200, 150), new TFPos(200, 150), "stream", 2, "stream", 1, false,
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
	}
}
