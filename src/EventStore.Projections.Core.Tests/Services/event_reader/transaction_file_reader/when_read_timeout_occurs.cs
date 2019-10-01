using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.Tests.Services.TimeService;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_reader.transaction_file_reader {
	public class when_read_timeout_occurs : TestFixtureWithExistingEvents {
		private TransactionFileEventReader _eventReader;
		private Guid _distributionCorrelationId;
		private Guid _readAllEventsForwardCorrelationId;

		protected override void Given() {
			TicksAreHandledImmediately();
		}

		private FakeTimeProvider _fakeTimeProvider;

		public when_read_timeout_occurs() {
			_distributionCorrelationId = Guid.NewGuid();
			_fakeTimeProvider = new FakeTimeProvider();
			_eventReader = new TransactionFileEventReader(_bus, _distributionCorrelationId, null, new TFPos(100, 50),
				_fakeTimeProvider,
				deliverEndOfTFPosition: false, stopOnEof: true);
			_eventReader.Resume();
			_readAllEventsForwardCorrelationId = Consumer.HandledMessages.OfType<ClientMessage.ReadAllEventsForward>()
				.Last().CorrelationId;
			_eventReader.Handle(
				new ProjectionManagementMessage.Internal.ReadTimeout(_readAllEventsForwardCorrelationId, "$all"));
			_eventReader.Handle(
				new ClientMessage.ReadAllEventsForwardCompleted(
					_readAllEventsForwardCorrelationId, ReadAllResult.Success, null,
					new[] {
						EventStore.Core.Data.ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								1, 50, Guid.NewGuid(), Guid.NewGuid(), 50, 0, "a", ExpectedVersion.Any,
								_fakeTimeProvider.Now,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type1", new byte[] {1}, new byte[] {2}), 100),
						EventStore.Core.Data.ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								2, 150, Guid.NewGuid(), Guid.NewGuid(), 150, 0, "b", ExpectedVersion.Any,
								_fakeTimeProvider.Now,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type1", new byte[] {1}, new byte[] {2}), 200),
					}, null, false, 100, new TFPos(200, 150), new TFPos(500, -1), new TFPos(100, 50), 500));
		}

		[Fact]
		public void should_not_deliver_events() {
			Assert.Equal(0,
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Count());
		}

		[Fact]
		public void should_attempt_another_read_for_the_timed_out_reads() {
			var readAllEventsForwardMessages = Consumer.HandledMessages.OfType<ClientMessage.ReadAllEventsForward>();

			Assert.Equal(readAllEventsForwardMessages.First().CorrelationId, _readAllEventsForwardCorrelationId);
			Assert.Equal(1, readAllEventsForwardMessages.Skip(1).Count());
		}
	}
}
