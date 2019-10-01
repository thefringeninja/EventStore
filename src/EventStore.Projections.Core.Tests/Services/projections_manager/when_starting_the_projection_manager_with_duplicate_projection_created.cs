using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Options;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Tests.Services.TimeService;
using EventStore.Core.Util;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Tests.Services.core_projection;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Services;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class
		when_starting_the_projection_manager_with_duplicate_projection_created : TestFixtureWithExistingEvents {
		private new ITimeProvider _timeProvider;
		private ProjectionManager _manager;
		private Guid _workerId;

		protected override void Given() {
			_workerId = Guid.NewGuid();
			ExistingEvent(ProjectionNamesBuilder.ProjectionsRegistrationStream, ProjectionEventTypes.ProjectionCreated,
				null, "projection1");
			ExistingEvent(ProjectionNamesBuilder.ProjectionsRegistrationStream, ProjectionEventTypes.ProjectionCreated,
				null, "projection1");
			ExistingEvent(
				"$projections-projection1", ProjectionEventTypes.ProjectionUpdated, null,
				@"{""Query"":""fromAll(); on_any(function(){});log('hello-from-projection-definition');"", ""Mode"":""3"", ""Enabled"":true, ""HandlerType"":""JS""}");
		}

		public when_starting_the_projection_manager_with_duplicate_projection_created() {
			_timeProvider = new FakeTimeProvider();
			var queues = new Dictionary<Guid, IPublisher> {{_workerId, _bus}};
			_manager = new ProjectionManager(
				_bus,
				_bus,
				queues,
				_timeProvider,
				ProjectionType.All,
				_ioDispatcher,
				TimeSpan.FromMinutes(Opts.ProjectionsQueryExpiryDefault));
			_bus.Subscribe<ClientMessage.WriteEventsCompleted>(_manager);
			_bus.Subscribe<ClientMessage.ReadStreamEventsBackwardCompleted>(_manager);
			_bus.Subscribe<ClientMessage.ReadStreamEventsForwardCompleted>(_manager);
			_manager.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			_manager.Handle(new ProjectionManagementMessage.ReaderReady());
		}

		public override void Dispose() {
			_manager.Dispose();
			base.Dispose();
		}

		[Fact]
		public void projection_status_is_preparing() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetStatistics(new PublishEnvelope(_bus), null, "projection1",
					true));
			Assert.Equal(
				ManagedProjectionState.Preparing,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().SingleOrDefault(
					v => v.Projections[0].Name == "projection1").Projections[0].MasterStatus);
		}

		[Fact]
		public void projection_id_is_latest() {
			_manager.Handle(
				new ProjectionManagementMessage.Command.GetStatistics(new PublishEnvelope(_bus), null, "projection1",
					true));
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ProjectionManagementMessage.Statistics>().SingleOrDefault(
					v => v.Projections[0].Name == "projection1").Projections[0].ProjectionId);
		}
	}
}
