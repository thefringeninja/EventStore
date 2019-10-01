using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer {
	public class when_handling_statistics_report_message : specification_with_projection_core_service_response_writer {
		private Guid _projectionId;
		private ProjectionStatistics _statistics;

		protected override void Given() {
			_projectionId = Guid.NewGuid();
			_statistics = new ProjectionStatistics {
				BufferedEvents = 100,
				CheckpointStatus = "checkpoint-status",
				CoreProcessingTime = 10,
				ResultStreamName = "result-stream",
				EffectiveName = "effective-name",
				Enabled = true,
				Epoch = 10,
				EventsProcessedAfterRestart = 12345,
				LastCheckpoint = "last-chgeckpoint",
				MasterStatus = ManagedProjectionState.Completed,
				Mode = ProjectionMode.OneTime,
				PartitionsCached = 123,
				Name = "name",
				Position = CheckpointTag.FromPosition(0, 1000, 900).ToString(),
				Progress = 100,
				ProjectionId = 1234,
				ReadsInProgress = 2,
				StateReason = "reason",
				Status = "status",
				Version = 1,
				WritePendingEventsAfterCheckpoint = 3,
				WritePendingEventsBeforeCheckpoint = 4,
				WritesInProgress = 5,
			};
		}

		protected override void When() {
			_sut.Handle(new CoreProjectionStatusMessage.StatisticsReport(_projectionId, _statistics, 0));
		}

		[Fact]
		public void publishes_statistics_report_response() {
			var command = AssertParsedSingleCommand<StatisticsReport>("$statistics-report");
			Assert.Equal(_projectionId.ToString("N"), command.Id);
			Assert.Equal(_statistics.BufferedEvents, command.Statistics.BufferedEvents);
			Assert.Equal(_statistics.CheckpointStatus, command.Statistics.CheckpointStatus);
			Assert.Equal(_statistics.CoreProcessingTime, command.Statistics.CoreProcessingTime);
			Assert.Equal(_statistics.EffectiveName, command.Statistics.EffectiveName);
			Assert.Equal(_statistics.Enabled, command.Statistics.Enabled);
			Assert.Equal(_statistics.Epoch, command.Statistics.Epoch);
			Assert.Equal(_statistics.EventsProcessedAfterRestart, command.Statistics.EventsProcessedAfterRestart);
			Assert.Equal(_statistics.LastCheckpoint, command.Statistics.LastCheckpoint);
			Assert.Equal(_statistics.MasterStatus, command.Statistics.MasterStatus);
			Assert.Equal(_statistics.Mode, command.Statistics.Mode);
			Assert.Equal(_statistics.Name, command.Statistics.Name);
			Assert.Equal(_statistics.PartitionsCached, command.Statistics.PartitionsCached);
			Assert.Equal(_statistics.Position, command.Statistics.Position);
			Assert.Equal(_statistics.Progress, command.Statistics.Progress);
			Assert.Equal(_statistics.ProjectionId, command.Statistics.ProjectionId);
			Assert.Equal(_statistics.ReadsInProgress, command.Statistics.ReadsInProgress);
			Assert.Equal(_statistics.StateReason, command.Statistics.StateReason);
			Assert.Equal(_statistics.Status, command.Statistics.Status);
			Assert.Equal(_statistics.Version, command.Statistics.Version);
			Assert.Equal(
				_statistics.WritePendingEventsAfterCheckpoint,
				command.Statistics.WritePendingEventsAfterCheckpoint);
			Assert.Equal(
				_statistics.WritePendingEventsBeforeCheckpoint,
				command.Statistics.WritePendingEventsBeforeCheckpoint);
			Assert.Equal(_statistics.WritesInProgress, command.Statistics.WritesInProgress);
			Assert.Equal(_statistics.ResultStreamName, command.Statistics.ResultStreamName);
		}
	}
}
