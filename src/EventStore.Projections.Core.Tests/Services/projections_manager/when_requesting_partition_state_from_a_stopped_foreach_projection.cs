using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using Xunit;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Services;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class when_requesting_partition_state_from_a_stopped_foreach_projection :
		TestFixtureWithProjectionCoreAndManagementServices {
		protected override void Given() {
			NoStream("$projections-test-projection-order");
			ExistingEvent(ProjectionNamesBuilder.ProjectionsRegistrationStream, ProjectionEventTypes.ProjectionCreated,
				null, "test-projection");
			ExistingEvent(
				"$projections-test-projection", ProjectionEventTypes.ProjectionUpdated, null,
				@"{""Query"":""fromCategory('test').foreachStream().when({'e': function(s,e){}})"", 
                    ""Mode"":""3"", ""Enabled"":false, ""HandlerType"":""JS"",
                    ""SourceDefinition"":{
                        ""AllEvents"":true,
                        ""AllStreams"":false,
                        ""Streams"":[""$ce-test""]
                    }
                }");
			ExistingEvent("$projections-test-projection-a-checkpoint", ProjectionEventTypes.PartitionCheckpoint,
				@"{""s"":{""$ce-test"": 9}}", @"{""data"":1}");
			NoStream("$projections-test-projection-b-checkpoint");
			ExistingEvent("$projections-test-projection-checkpoint", ProjectionEventTypes.ProjectionCheckpoint,
				@"{""s"":{""$ce-test"": 10}}", @"{}");
			AllWritesSucceed();
		}

		private string _projectionName;

		protected override IEnumerable<WhenStep> When() {
			_projectionName = "test-projection";
			// when
			yield return (new SystemMessage.BecomeMaster(Guid.NewGuid()));
			yield return (new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now)));
			yield return (new SystemMessage.SystemCoreReady());
		}

		[Fact]
		public void the_projection_state_can_be_retrieved() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetState(new PublishEnvelope(_bus), _projectionName, "a"));
			Queue.Process();

			Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Count());

			var first = Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().First();
			Assert.Equal(_projectionName, first.Name);
			Assert.Equal(@"{""data"":1}", first.State);

			_manager.Handle(
				new ProjectionManagementMessage.Command.GetState(new PublishEnvelope(_bus), _projectionName, "b"));
			Queue.Process();

			Assert.Equal(2, Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Count());
			var second = Consumer.HandledMessages.OfType<ProjectionManagementMessage.ProjectionState>().Skip(1)
				.First();
			Assert.Equal(_projectionName, second.Name);
			Assert.Equal("", second.State);
		}
	}
}
