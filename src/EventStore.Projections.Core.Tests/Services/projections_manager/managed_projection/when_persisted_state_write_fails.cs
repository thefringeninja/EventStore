using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.Services.TimeService;
using EventStore.Core.Util;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.managed_projection {
	public class when_persisted_state_write_fails : TestFixtureWithExistingEvents {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {OperationResult.CommitTimeout};
			yield return new object[] {OperationResult.ForwardTimeout};
			yield return new object[] {OperationResult.PrepareTimeout};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void should_retry_writing_the_persisted_state_with_the_same_event_id(OperationResult operationResult) {
			using var fixture = new Fixture(operationResult);
			var eventId = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
				.Where(x => x.EventStreamId == fixture.ProjectionDefinitionStreamId).First().Events[0].EventId;
			Assert.Equal(eventId, fixture.OriginalPersistedStateEventId);
		}

		class Fixture : TestFixtureWithExistingEvents {
			private new ITimeProvider _timeProvider;
			private ManagedProjection _managedProjection;
			private Guid _coreProjectionId;
			private string _projectionName;
			public string ProjectionDefinitionStreamId;
			public Guid OriginalPersistedStateEventId;

			private OperationResult _failureCondition;

			public Fixture(OperationResult failureCondition) : this() {
				_failureCondition = failureCondition;
			}

			protected override ManualQueue GiveInputQueue() {
				return new ManualQueue(_bus, _timeProvider);
			}

			public Fixture() {
				AllWritesQueueUp();
				WhenLoop();
			}

			protected override void Given() {
				_projectionName = "projectionName";
				ProjectionDefinitionStreamId = ProjectionNamesBuilder.ProjectionsStreamPrefix + _projectionName;
				_coreProjectionId = Guid.NewGuid();
				_timeProvider = new FakeTimeProvider();
				_managedProjection = new ManagedProjection(
					Guid.NewGuid(),
					Guid.NewGuid(),
					1,
					_projectionName,
					true,
					null,
					_streamDispatcher,
					_writeDispatcher,
					_readDispatcher,
					_bus,
					_timeProvider, new RequestResponseDispatcher
						<CoreProjectionManagementMessage.GetState, CoreProjectionStatusMessage.StateReport>(
							_bus,
							v => v.CorrelationId,
							v => v.CorrelationId,
							new PublishEnvelope(_bus)),
					new RequestResponseDispatcher
						<CoreProjectionManagementMessage.GetResult, CoreProjectionStatusMessage.ResultReport>(
							_bus,
							v => v.CorrelationId,
							v => v.CorrelationId,
							new PublishEnvelope(_bus)),
					_ioDispatcher,
					TimeSpan.FromMinutes(Opts.ProjectionsQueryExpiryDefault));
			}

			protected override IEnumerable<WhenStep> When() {
				ProjectionManagementMessage.Command.Post message = new ProjectionManagementMessage.Command.Post(
					Envelope, ProjectionMode.OneTime, _projectionName, ProjectionManagementMessage.RunAs.System,
					typeof(FakeForeachStreamProjection), "", true, false, false, false);
				_managedProjection.InitializeNew(
					new ManagedProjection.PersistedState {
						Enabled = message.Enabled,
						HandlerType = message.HandlerType,
						Query = message.Query,
						Mode = message.Mode,
						EmitEnabled = message.EmitEnabled,
						CheckpointsDisabled = !message.CheckpointsEnabled,
						Epoch = -1,
						Version = -1,
						RunAs = message.EnableRunAs ? SerializedRunAs.SerializePrincipal(message.RunAs) : null,
					},
					null);

				var sourceDefinition = new FakeForeachStreamProjection("", Console.WriteLine).GetSourceDefinition();
				var projectionSourceDefinition = ProjectionSourceDefinition.From(sourceDefinition);

				_managedProjection.Handle(
					new CoreProjectionStatusMessage.Prepared(
						_coreProjectionId, projectionSourceDefinition));

				OriginalPersistedStateEventId = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.Where(x => x.EventStreamId == ProjectionDefinitionStreamId).First().Events[0].EventId;

				CompleteWriteWithResult(_failureCondition);

				Consumer.HandledMessages.Clear();

				yield break;
			}
		}
	}
}
