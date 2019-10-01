using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader {
	public class
		when_receiving_statistics_report_response : specification_with_projection_manager_response_reader_started {
		private Guid _projectionId;
		private int _bufferedEvents;
		private string _checkpointStatus;
		private int _coreProcessingTime;
		private string _resultStreamName;
		private string _effectiveName;
		private bool _enabled;
		private int _epoch;
		private int _eventsProcessedAfterRestart;
		private string _lastCheckpoint;
		private ManagedProjectionState _masterStatus;
		private ProjectionMode _mode;
		private int _partitionsCached;
		private string _name;
		private string _position;
		private int _progress;
		private int _projectionIdNum;
		private int _readsInProgress;
		private string _stateReason;
		private string _status;
		private int _version;
		private int _writePendingEventsAfterCheckpoint;
		private int _writePendingEventsBeforeCheckpoint;
		private int _writesInProgress;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			_bufferedEvents = 100;
			_checkpointStatus = "checkpoint-status";
			_coreProcessingTime = 10;
			_resultStreamName = "result-stream";
			_effectiveName = "effective-name";
			_enabled = true;
			_epoch = 10;
			_eventsProcessedAfterRestart = 12345;
			_lastCheckpoint = "last-chgeckpoint";
			_masterStatus = ManagedProjectionState.Completed;
			_mode = ProjectionMode.OneTime;
			_partitionsCached = 123;
			_name = "name";
			_position = CheckpointTag.FromPosition(0, 1000, 900).ToString();
			_progress = 100;
			_projectionIdNum = 1234;
			_readsInProgress = 2;
			_stateReason = "reason";
			_status = "status";
			_version = 1;
			_writePendingEventsAfterCheckpoint = 3;
			_writePendingEventsBeforeCheckpoint = 4;
			_writesInProgress = 5;

			yield return CreateWriteEvent("$projections-$master", "$statistics-report", @"{
                ""id"":""" + _projectionId.ToString("N") + @""",
                ""statistics"":{
                        ""bufferedEvents"":""" + _bufferedEvents + @""",
                        ""checkpointStatus"":""" + _checkpointStatus + @""",
                        ""coreProcessingTime"":""" + _coreProcessingTime + @""",
                        ""resultStreamName"":""" + _resultStreamName + @""",
                        ""effectiveName"":""" + _effectiveName + @""",
                        ""enabled"":""" + _enabled + @""",
                        ""epoch"":""" + _epoch + @""",
                        ""eventsProcessedAfterRestart"":""" + _eventsProcessedAfterRestart + @""",
                        ""lastCheckpoint"":""" + _lastCheckpoint + @""",
                        ""masterStatus"":""" + _masterStatus + @""",
                        ""mode"":""" + _mode + @""",
                        ""partitionsCached"":""" + _partitionsCached + @""",
                        ""name"":""" + _name + @""",
                        ""position"":""" + _position + @""",
                        ""progress"":""" + _progress + @""",
                        ""projectionId"":""" + _projectionIdNum + @""",
                        ""readsInProgress"":""" + _readsInProgress + @""",
                        ""stateReason"":""" + _stateReason + @""",
                        ""status"":""" + _status + @""",
                        ""version"":""" + _version + @""",
                        ""writePendingEventsAfterCheckpoint"":""" + _writePendingEventsAfterCheckpoint + @""",
                        ""writePendingEventsBeforeCheckpoint"":""" + _writePendingEventsBeforeCheckpoint + @""",
                        ""writesInProgress"":""" + _writesInProgress + @""",
                }
            }", null, true);
		}

		[Fact]
		public void publishes_statistics_report_message() {
			var response =
				HandledMessages.OfType<CoreProjectionStatusMessage.StatisticsReport>().LastOrDefault();
			Assert.NotNull(response);
			Assert.Equal(_projectionId, response.ProjectionId);
			Assert.Equal(_bufferedEvents, response.Statistics.BufferedEvents);
			Assert.Equal(_checkpointStatus, response.Statistics.CheckpointStatus);
			Assert.Equal(_coreProcessingTime, response.Statistics.CoreProcessingTime);
			Assert.Equal(_resultStreamName, response.Statistics.ResultStreamName);
			Assert.Equal(_effectiveName, response.Statistics.EffectiveName);
			Assert.Equal(_enabled, response.Statistics.Enabled);
			Assert.Equal(_epoch, response.Statistics.Epoch);
			Assert.Equal(_eventsProcessedAfterRestart, response.Statistics.EventsProcessedAfterRestart);
			Assert.Equal(_lastCheckpoint, response.Statistics.LastCheckpoint);
			Assert.Equal(_masterStatus, response.Statistics.MasterStatus);
			Assert.Equal(_mode, response.Statistics.Mode);
			Assert.Equal(_partitionsCached, response.Statistics.PartitionsCached);
			Assert.Equal(_name, response.Statistics.Name);
			Assert.Equal(_position, response.Statistics.Position);
			Assert.Equal(_progress, response.Statistics.Progress);
			Assert.Equal(_projectionIdNum, response.Statistics.ProjectionId);
			Assert.Equal(_readsInProgress, response.Statistics.ReadsInProgress);
			Assert.Equal(_stateReason, response.Statistics.StateReason);
			Assert.Equal(_status, response.Statistics.Status);
			Assert.Equal(_version, response.Statistics.Version);
			Assert.Equal(_writePendingEventsAfterCheckpoint, response.Statistics.WritePendingEventsAfterCheckpoint);
			Assert.Equal(_writePendingEventsBeforeCheckpoint,
				response.Statistics.WritePendingEventsBeforeCheckpoint);
			Assert.Equal(_writesInProgress, response.Statistics.WritesInProgress);
		}
	}
}
