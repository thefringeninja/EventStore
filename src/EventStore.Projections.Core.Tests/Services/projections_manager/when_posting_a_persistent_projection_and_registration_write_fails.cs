using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using Xunit;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class when_posting_a_persistent_projection_and_registration_write_fails {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {OperationResult.CommitTimeout};
			yield return new object[] {OperationResult.ForwardTimeout};
			yield return new object[] {OperationResult.PrepareTimeout};
		}

		[Theory, MemberData(nameof(TestCases)), Trait("Category", "v8")]
		public void retries_creating_the_projection_only_the_specified_number_of_times_and_the_same_event_id(
			OperationResult operationResult) {
			using var fixture = new Fixture(operationResult);
			int retryCount = 0;
			var projectionRegistrationWrite = fixture.Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
				.Where(x => x.EventStreamId == ProjectionNamesBuilder.ProjectionsRegistrationStream).Last();
			var eventId = projectionRegistrationWrite.Events[0].EventId;
			while (projectionRegistrationWrite != null) {
				Assert.Equal(eventId, projectionRegistrationWrite.Events[0].EventId);
				projectionRegistrationWrite.Envelope.ReplyWith(new ClientMessage.WriteEventsCompleted(
					projectionRegistrationWrite.CorrelationId, fixture.FailureCondition,
					Enum.GetName(typeof(OperationResult), fixture.FailureCondition)));
				fixture.Queue.Process();
				projectionRegistrationWrite = fixture.Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.Where(x => x.EventStreamId == ProjectionNamesBuilder.ProjectionsRegistrationStream)
					.LastOrDefault();
				if (projectionRegistrationWrite != null) {
					retryCount++;
				}

				fixture.Consumer.HandledMessages.Clear();
			}

			Assert.Equal(ProjectionManager.ProjectionCreationRetryCount, retryCount);
		}

		class Fixture : TestFixtureWithProjectionCoreAndManagementServices {
			public OperationResult FailureCondition;

			public Fixture(OperationResult failureCondition) {
				FailureCondition = failureCondition;
			}

			protected override void Given() {
				NoStream("$projections-test-projection-order");
				AllWritesToSucceed("$projections-test-projection-order");
				NoStream("$projections-test-projection-checkpoint");
				NoOtherStreams();
				AllWritesQueueUp();
			}

			private string _projectionName;

			protected override IEnumerable<WhenStep> When() {
				_projectionName = "test-projection";
				yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
				yield return new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now));
				yield return new SystemMessage.SystemCoreReady();
				yield return
					new ProjectionManagementMessage.Command.Post(
						new PublishEnvelope(_bus), ProjectionMode.Continuous, _projectionName,
						ProjectionManagementMessage.RunAs.System, "JS",
						@"fromAll().when({$any:function(s,e){return s;}});",
						enabled: true, checkpointsEnabled: true, emitEnabled: true, trackEmittedStreams: true);
			}
		}
	}
}
