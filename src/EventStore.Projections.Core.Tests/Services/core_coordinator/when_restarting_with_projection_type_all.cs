using System;
using System.Linq;
using Xunit;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Management;
using EventStore.Common.Options;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Services.Replication;
using System.Collections.Generic;

namespace EventStore.Projections.Core.Tests.Services.core_coordinator {
	public class when_restarting_with_projection_type_all {
		private FakePublisher[] queues;
		private FakePublisher publisher;
		private ProjectionCoreCoordinator _coordinator;
		private TimeoutScheduler[] timeoutScheduler = { };
		private FakeEnvelope envelope = new FakeEnvelope();

		public when_restarting_with_projection_type_all() {
			queues = new List<FakePublisher>() {new FakePublisher()}.ToArray();
			publisher = new FakePublisher();

			_coordinator =
				new ProjectionCoreCoordinator(ProjectionType.All, timeoutScheduler, queues, publisher, envelope);
			_coordinator.Handle(new SystemMessage.SystemCoreReady());
			_coordinator.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			_coordinator.Handle(new SystemMessage.EpochWritten(new EpochRecord(0, 0, Guid.NewGuid(), 0, DateTime.Now)));

			//force stop
			_coordinator.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			//clear queues for clearer testing
			queues[0].Messages.Clear();
		}

		private void BecomeReady() {
			//become ready
			_coordinator.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			_coordinator.Handle(new SystemMessage.EpochWritten(new EpochRecord(0, 0, Guid.NewGuid(), 0, DateTime.Now)));
		}

		private void AllSubComponentsStarted() {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("EventReaderCoreService"));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreService"));
			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreServiceCommandReader"));
		}

		private void AllSubComponentsStopped() {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService"));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService"));
			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreServiceCommandReader"));
		}

		[Fact]
		public void should_not_start_if_subcomponents_not_stopped() {
			AllSubComponentsStarted();

			BecomeReady();
			Assert.Empty(queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader));
			Assert.Empty(queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore));
		}

		[Fact]
		public void should_not_start_if_only_some_subcomponents_stopped() {
			AllSubComponentsStarted();

			/*Only some subcomponents stopped*/
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService"));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService"));

			BecomeReady();
			Assert.Empty(queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader));
			Assert.Empty(queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore));
		}

		[Fact]
		public void should_start_if_subcomponents_stopped_before_becoming_ready() {
			AllSubComponentsStarted();

			AllSubComponentsStopped();
			BecomeReady();
			Assert.Single(queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader));
			Assert.Single(queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore));
		}

		[Fact]
		public void should_start_if_subcomponents_stopped_after_becoming_ready() {
			AllSubComponentsStarted();

			BecomeReady();
			AllSubComponentsStopped();
			Assert.Single(queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader));
			Assert.Single(queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore));
		}

		[Fact]
		public void should_start_if_some_subcomponents_stopped_before_becoming_ready_and_some_after_becoming_ready() {
			AllSubComponentsStarted();

			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService"));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService"));

			BecomeReady();

			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreServiceCommandReader"));

			Assert.Single(queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader));
			Assert.Single(queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore));
		}

		[Fact]
		public void should_start_if_subcomponents_started_and_stopped_late_after_becoming_ready() {
			BecomeReady();
			AllSubComponentsStarted();
			AllSubComponentsStopped();
			Assert.Single(queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader));
			Assert.Single(queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore));
		}

		[Fact]
		public void should_start_if_subcomponents_started_and_stopped_in_a_different_random_order() {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("EventReaderCoreService"));
			BecomeReady();

			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreService"));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService"));

			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreServiceCommandReader"));


			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreServiceCommandReader"));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService"));

			Assert.Single(queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader));
			Assert.Single(queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore));
		}
	}
}
