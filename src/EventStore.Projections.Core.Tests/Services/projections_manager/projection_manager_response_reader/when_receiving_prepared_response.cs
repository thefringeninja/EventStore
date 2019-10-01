using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader {
	public class when_receiving_prepared_response : specification_with_projection_manager_response_reader_started {
		private const string Query = @"fromStream('$user-admin').outputState()";
		private Guid _projectionId;

		protected override IEnumerable<WhenStep> When() {
			_projectionId = Guid.NewGuid();
			yield return
				CreateWriteEvent(
					"$projections-$master",
					"$prepared",
					@"{
                        ""id"":""" + _projectionId.ToString("N") + @""",
                         ""sourceDefinition"":{
                             ""allEvents"":false,   
                             ""allStreams"":false,
                             ""byStream"":true,
                             ""byCustomPartitions"":false,
                             ""categories"":[""account""],
                             ""events"":[""added"",""removed""],
                             ""streams"":[],
                             ""catalogStream"":"""",
                             ""limitingCommitPosition"":100000,
                             ""options"":{
                                 ""resultStreamName"":""ResultStreamName"",
                                 ""partitionResultStreamNamePattern"":""PartitionResultStreamNamePattern"",
                                 ""reorderEvents"":false,
                                 ""processingLag"":0,
                                 ""isBiState"":false,
                                 ""definesStateTransform"":false,
                                 ""definesCatalogTransform"":false,
                                 ""producesResults"":true,
                                 ""definesFold"":false,
                                 ""handlesDeletedNotifications"":false,
                                 ""includeLinks"":true,
                                 ""disableParallelism"":true,
                             },
                         },
                         ""version"":{},
                         ""handlerType"":""JS"",
                         ""query"":""" + Query + @""",
                         ""name"":""test""
                    }",
					null,
					true);
		}

		[Fact]
		public void publishes_prepared_message() {
			var createPrepared =
				HandledMessages.OfType<CoreProjectionStatusMessage.Prepared>().LastOrDefault();
			Assert.NotNull(createPrepared);
			Assert.Equal(_projectionId, createPrepared.ProjectionId);
			var projectionSourceDefinition = createPrepared.SourceDefinition as IQuerySources;
			Assert.NotNull(projectionSourceDefinition);
			Assert.False(projectionSourceDefinition.AllEvents);
			Assert.False(projectionSourceDefinition.AllStreams);
			Assert.Equal(true, projectionSourceDefinition.ByStreams);
			Assert.False(projectionSourceDefinition.ByCustomPartitions);
			Assert.True(new[] {"account"}.SequenceEqual(projectionSourceDefinition.Categories));
			Assert.True(new[] {"added", "removed"}.SequenceEqual(projectionSourceDefinition.Events));
			Assert.True(new string[] { }.SequenceEqual(projectionSourceDefinition.Streams));
			Assert.Equal("", projectionSourceDefinition.CatalogStream);
			Assert.Equal(100000, projectionSourceDefinition.LimitingCommitPosition);
			Assert.Equal("ResultStreamName", projectionSourceDefinition.ResultStreamNameOption);
			Assert.Equal(
				"PartitionResultStreamNamePattern",
				projectionSourceDefinition.PartitionResultStreamNamePatternOption);
			Assert.False(projectionSourceDefinition.ReorderEventsOption);
			Assert.Equal(0, projectionSourceDefinition.ProcessingLagOption);
			Assert.False(projectionSourceDefinition.IsBiState);
			Assert.False(projectionSourceDefinition.DefinesStateTransform);
			Assert.False(projectionSourceDefinition.DefinesCatalogTransform);
			Assert.Equal(true, projectionSourceDefinition.ProducesResults);
			Assert.False(projectionSourceDefinition.DefinesFold);
			Assert.False(projectionSourceDefinition.HandlesDeletedNotifications);
			Assert.Equal(true, projectionSourceDefinition.IncludeLinksOption);
			Assert.Equal(true, projectionSourceDefinition.DisableParallelismOption);
		}
	}
}
