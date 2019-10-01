using System;
using EventStore.Core.Authentication;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Commands;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.command_writer {
	public class when_handling_create_prepared_message : specification_with_projection_manager_command_writer {
		private Guid _projectionId;
		private Guid _workerId;
		private ProjectionConfig _config;
		private QuerySourcesDefinition _definition;
		private ProjectionVersion _projectionVersion;
		private string _projectionName;
		private string _handlerType;
		private string _query;

		protected override void Given() {
			_projectionId = Guid.NewGuid();
			_workerId = Guid.NewGuid();
			_projectionName = "projection";
			_handlerType = "JS";
			_query = "from()";

			_config = new ProjectionConfig(
				new OpenGenericPrincipal("user", "a", "b"),
				1000,
				100000,
				2000,
				200,
				true,
				true,
				true,
				true,
				true,
				true,
				10000,
				1);

			var builder = new SourceDefinitionBuilder();
			builder.FromStream("s1");
			builder.FromStream("s2");
			builder.IncludeEvent("e1");
			builder.IncludeEvent("e2");
			builder.SetByStream();
			builder.SetResultStreamNameOption("result-stream");
			_definition = QuerySourcesDefinition.From(builder);
		}

		protected override void When() {
			_projectionVersion = new ProjectionVersion(1, 2, 3);
			_sut.Handle(
				new CoreProjectionManagementMessage.CreatePrepared(
					_projectionId,
					_workerId,
					_projectionName,
					_projectionVersion,
					_config,
					_definition,
					_handlerType,
					_query));
		}

		[Fact]
		public void publishes_create_prepared_command() {
			var command =
				AssertParsedSingleCommand<CreatePreparedCommand>(
					"$create-prepared",
					_workerId);
			Assert.Equal(_projectionId.ToString("N"), command.Id);
			Assert.Equal(_handlerType, command.HandlerType);
			Assert.Equal(_projectionName, command.Name);
			Assert.Equal(_query, command.Query);
			Assert.Equal(
				(PersistedProjectionVersion)_projectionVersion,
				command.Version);
			Assert.Equal(_config.CheckpointHandledThreshold, command.Config.CheckpointHandledThreshold);
			Assert.Equal(_config.CheckpointUnhandledBytesThreshold,
				command.Config.CheckpointUnhandledBytesThreshold);
			Assert.Equal(_config.CheckpointsEnabled, command.Config.CheckpointsEnabled);
			Assert.Equal(_config.CreateTempStreams, command.Config.CreateTempStreams);
			Assert.Equal(_config.EmitEventEnabled, command.Config.EmitEventEnabled);
			Assert.Equal(_config.IsSlaveProjection, command.Config.IsSlaveProjection);
			Assert.Equal(_config.MaxWriteBatchLength, command.Config.MaxWriteBatchLength);
			Assert.Equal(_config.PendingEventsThreshold, command.Config.PendingEventsThreshold);
			Assert.Equal(_config.MaximumAllowedWritesInFlight, command.Config.MaximumAllowedWritesInFlight);
			Assert.Equal(_config.RunAs.Identity.Name, command.Config.RunAs);
			Assert.Equal(_config.StopOnEof, command.Config.StopOnEof);
			Assert.Equal(_definition.AllEvents, command.SourceDefinition.AllEvents);
			Assert.Equal(_definition.AllStreams, command.SourceDefinition.AllStreams);
			Assert.Equal(_definition.ByCustomPartitions, command.SourceDefinition.ByCustomPartitions);
			Assert.Equal(_definition.ByStreams, command.SourceDefinition.ByStreams);
			Assert.Equal(_definition.CatalogStream, command.SourceDefinition.CatalogStream);
			Assert.Equal(_definition.Categories, command.SourceDefinition.Categories);
			Assert.Equal(_definition.Events, command.SourceDefinition.Events);
			Assert.Equal(_definition.LimitingCommitPosition, command.SourceDefinition.LimitingCommitPosition);
			Assert.Equal(_definition.Streams, command.SourceDefinition.Streams);
			Assert.Equal(
				_definition.Options.DefinesCatalogTransform,
				command.SourceDefinition.Options.DefinesCatalogTransform);
			Assert.Equal(_definition.Options.DefinesFold, command.SourceDefinition.Options.DefinesFold);
			Assert.Equal(
				_definition.Options.DefinesStateTransform,
				command.SourceDefinition.Options.DefinesStateTransform);
			Assert.Equal(_definition.Options.DisableParallelism,
				command.SourceDefinition.Options.DisableParallelism);
			Assert.Equal(
				_definition.Options.HandlesDeletedNotifications,
				command.SourceDefinition.Options.HandlesDeletedNotifications);
			Assert.Equal(_definition.Options.IncludeLinks, command.SourceDefinition.Options.IncludeLinks);
			Assert.Equal(_definition.Options.IsBiState, command.SourceDefinition.Options.IsBiState);
			Assert.Equal(
				_definition.Options.PartitionResultStreamNamePattern,
				command.SourceDefinition.Options.PartitionResultStreamNamePattern);
			Assert.Equal(_definition.Options.ProcessingLag, command.SourceDefinition.Options.ProcessingLag);
			Assert.Equal(_definition.Options.ProducesResults, command.SourceDefinition.Options.ProducesResults);
			Assert.Equal(_definition.Options.ReorderEvents, command.SourceDefinition.Options.ReorderEvents);
			Assert.Equal(_definition.Options.ResultStreamName, command.SourceDefinition.Options.ResultStreamName);
		}
	}
}
