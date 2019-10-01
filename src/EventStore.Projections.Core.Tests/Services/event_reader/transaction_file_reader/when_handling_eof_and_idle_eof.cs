using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Tests.Services.TimeService;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using EventStore.Core.Services.AwakeReaderService;

namespace EventStore.Projections.Core.Tests.Services.event_reader.transaction_file_reader {
	public class when_handling_eof_and_idle_eof : TestFixtureWithExistingEvents {
		private TransactionFileEventReader _edp;
		private Guid _distibutionPointCorrelationId;
		private Guid _firstEventId;
		private Guid _secondEventId;

		protected override void Given() {
			TicksAreHandledImmediately();
		}

		private FakeTimeProvider _fakeTimeProvider;

		public when_handling_eof_and_idle_eof() {
			_distibutionPointCorrelationId = Guid.NewGuid();
			_fakeTimeProvider = new FakeTimeProvider();
			_edp = new TransactionFileEventReader(_bus, _distibutionPointCorrelationId, null, new TFPos(100, 50),
				_fakeTimeProvider,
				deliverEndOfTFPosition: false);
			_edp.Resume();
			_firstEventId = Guid.NewGuid();
			_secondEventId = Guid.NewGuid();
			var correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadAllEventsForward>().Last()
				.CorrelationId;
			_edp.Handle(
				new ClientMessage.ReadAllEventsForwardCompleted(
					correlationId, ReadAllResult.Success, null,
					new[] {
						EventStore.Core.Data.ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								1, 50, Guid.NewGuid(), _firstEventId, 50, 0, "a", ExpectedVersion.Any,
								_fakeTimeProvider.Now,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type1", new byte[] {1}, new byte[] {2}), 100),
						EventStore.Core.Data.ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								2, 150, Guid.NewGuid(), _secondEventId, 150, 0, "b", ExpectedVersion.Any,
								_fakeTimeProvider.Now,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type1", new byte[] {1}, new byte[] {2}), 200),
					}, null, false, 100,
					new TFPos(200, 150), new TFPos(500, -1), new TFPos(100, 50), 500));
			correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadAllEventsForward>().Last().CorrelationId;
			_edp.Handle(
				new ClientMessage.ReadAllEventsForwardCompleted(
					correlationId, ReadAllResult.Success, null,
					new EventStore.Core.Data.ResolvedEvent[0], null, false, 100, new TFPos(), new TFPos(), new TFPos(),
					500));
			_fakeTimeProvider.AddTime(TimeSpan.FromMilliseconds(500));
			correlationId = ((ClientMessage.ReadAllEventsForward)(Consumer.HandledMessages
				.OfType<AwakeServiceMessage.SubscribeAwake>().Last().ReplyWithMessage)).CorrelationId;
			_edp.Handle(
				new ClientMessage.ReadAllEventsForwardCompleted(
					correlationId, ReadAllResult.Success, null,
					new EventStore.Core.Data.ResolvedEvent[0], null, false, 100, new TFPos(), new TFPos(), new TFPos(),
					500));
		}

		[Fact]
		public void publishes_event_distribution_idle_messages() {
			Assert.Equal(
				2, Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.EventReaderIdle>().Count());
			var first =
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.EventReaderIdle>().First();
			var second =
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.EventReaderIdle>()
					.Skip(1)
					.First();

			Assert.Equal(first.CorrelationId, _distibutionPointCorrelationId);
			Assert.Equal(second.CorrelationId, _distibutionPointCorrelationId);

			Assert.Equal(TimeSpan.FromMilliseconds(500), second.IdleTimestampUtc - first.IdleTimestampUtc);
		}
	}
}
