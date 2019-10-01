using System;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.TimerService;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;
using System.Linq;
using EventStore.Projections.Core.Messages;

namespace EventStore.Projections.Core.Tests.Services.event_reader.stream_reader {
	public class when_handling_soft_deleted_stream_with_a_single_event_event_reader : TestFixtureWithExistingEvents {
		private StreamEventReader _streamEventReader;
		private Guid _distibutionPointCorrelationId;
		private Guid _firstEventId;
		private Guid _secondEventId;
		private string _streamId = Guid.NewGuid().ToString();

		protected override void Given() {
			TicksAreHandledImmediately();

			_distibutionPointCorrelationId = Guid.NewGuid();
			_streamEventReader = new StreamEventReader(_bus, _distibutionPointCorrelationId, null, _streamId, 0,
				new RealTimeProvider(), false,
				produceStreamDeletes: false);
			_streamEventReader.Resume();
			_firstEventId = Guid.NewGuid();
			_secondEventId = Guid.NewGuid();
		}

		public when_handling_soft_deleted_stream_with_a_single_event_event_reader() {
			var correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last()
				.CorrelationId;
			_streamEventReader.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					correlationId, _streamId, 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								10, 50, Guid.NewGuid(), _firstEventId, 50, 0, _streamId, ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type1", new byte[] {1}, new byte[] {2})),
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								11, 100, Guid.NewGuid(), _secondEventId, 100, 0, _streamId, ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd |
								PrepareFlags.IsJson,
								"event_type2", new byte[] {3}, new byte[] {4}))
					}, null, false, "", 12, 11, true, 200));
		}

		[Fact]
		public void should_handle_the_2_events() {
			Assert.Equal(2,
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Count());

			var first = Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().First();
			Assert.Equal(first.Data.EventId, _firstEventId);
			var second = Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Skip(1)
				.First();
			Assert.Equal(second.Data.EventId, _secondEventId);
		}
	}
}
