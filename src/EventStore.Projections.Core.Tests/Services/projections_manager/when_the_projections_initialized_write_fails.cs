using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using Xunit;
using EventStore.Projections.Core.Services.Processing;
using System.Collections;
using EventStore.Projections.Core.Services;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class when_writing_the_projections_initialized_event_fails {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {OperationResult.CommitTimeout};
			yield return new object[] {OperationResult.ForwardTimeout};
			yield return new object[] {OperationResult.PrepareTimeout};
		}

		[Theory, MemberData(nameof(TestCases)), Trait("Category", "v8")]
		public void retries_writing_with_the_same_event_id(OperationResult operationResult) {
			using var fixture = new Fixture(operationResult);
			int retryCount = 0;
			var projectionsInitializedWrite = fixture.Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
				.Where(x =>
					x.EventStreamId == ProjectionNamesBuilder.ProjectionsRegistrationStream &&
					x.Events[0].EventType == ProjectionEventTypes.ProjectionsInitialized).Last();
			var eventId = projectionsInitializedWrite.Events[0].EventId;
			while (retryCount < 5) {
				Assert.Equal(eventId, projectionsInitializedWrite.Events[0].EventId);
				projectionsInitializedWrite.Envelope.ReplyWith(new ClientMessage.WriteEventsCompleted(
					projectionsInitializedWrite.CorrelationId, operationResult,
					Enum.GetName(typeof(OperationResult), operationResult)));
				fixture.Queue.Process();
				projectionsInitializedWrite = fixture.Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.Where(x =>
						x.EventStreamId == ProjectionNamesBuilder.ProjectionsRegistrationStream &&
						x.Events[0].EventType == ProjectionEventTypes.ProjectionsInitialized).LastOrDefault();
				if (projectionsInitializedWrite != null) {
					retryCount++;
				}

				fixture.Consumer.HandledMessages.Clear();
			}
		}

		class Fixture : TestFixtureWithProjectionCoreAndManagementServices {
			private OperationResult _failureCondition;

			public Fixture(OperationResult failureCondition) {
				_failureCondition = failureCondition;
			}

			protected override void Given() {
				AllWritesQueueUp();
				NoStream(ProjectionNamesBuilder.ProjectionsRegistrationStream);
			}

			protected override bool GivenInitializeSystemProjections() {
				return false;
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
				yield return new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now));
				yield return new SystemMessage.SystemCoreReady();
			}
		}
	}
}
