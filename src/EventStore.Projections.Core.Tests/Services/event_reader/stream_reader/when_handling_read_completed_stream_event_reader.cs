using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.TimerService;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.event_reader.stream_reader {
	public class when_handling_read_completed_stream_event_reader : TestFixtureWithExistingEvents {
		private StreamEventReader _edp;
		private Guid _distibutionPointCorrelationId;
		private Guid _firstEventId;
		private Guid _secondEventId;

		protected override void Given() {
			TicksAreHandledImmediately();
		}

		public when_handling_read_completed_stream_event_reader() {
			_distibutionPointCorrelationId = Guid.NewGuid();
			_edp = new StreamEventReader(_bus, _distibutionPointCorrelationId, null, "stream", 10,
				new RealTimeProvider(), false,
				produceStreamDeletes: false);
			_edp.Resume();
			_firstEventId = Guid.NewGuid();
			_secondEventId = Guid.NewGuid();
			var correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last()
				.CorrelationId;
			_edp.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					correlationId, "stream", 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								10, 50, Guid.NewGuid(), _firstEventId, 50, 0, "stream", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type1", new byte[] {1}, new byte[] {2})),
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								11, 100, Guid.NewGuid(), _secondEventId, 100, 0, "stream", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd |
								PrepareFlags.IsJson,
								"event_type2", new byte[] {3}, new byte[] {4}))
					}, null, false, "", 12, 11, true, 200));
		}

		[Fact]
		public void cannot_be_resumed() {
			Assert.Throws<InvalidOperationException>(() => { _edp.Resume(); });
		}

		[Fact]
		public void cannot_be_paused() {
			_edp.Pause();
		}

		[Fact]
		public void publishes_correct_committed_event_received_messages() {
			Assert.Equal(
				2, Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Count());
			var first =
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().First();
			var second =
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>()
					.Skip(1)
					.First();

			Assert.Equal("event_type1", first.Data.EventType);
			Assert.Equal("event_type2", second.Data.EventType);
			Assert.Equal(_firstEventId, first.Data.EventId);
			Assert.Equal(_secondEventId, second.Data.EventId);
			Assert.Equal(1, first.Data.Data[0]);
			Assert.Equal(2, first.Data.Metadata[0]);
			Assert.Equal(3, second.Data.Data[0]);
			Assert.Equal(4, second.Data.Metadata[0]);
			Assert.Equal("stream", first.Data.EventStreamId);
			Assert.Equal("stream", second.Data.EventStreamId);
			Assert.Equal(50, first.Data.Position.PreparePosition);
			Assert.Equal(100, second.Data.Position.PreparePosition);
			Assert.Equal(-1, first.Data.Position.CommitPosition);
			Assert.Equal(-1, second.Data.Position.CommitPosition);
			Assert.Equal(50, first.Data.EventOrLinkTargetPosition.PreparePosition);
			Assert.Equal(100, second.Data.EventOrLinkTargetPosition.PreparePosition);
			Assert.Equal(-1, first.Data.EventOrLinkTargetPosition.CommitPosition);
			Assert.Equal(-1, second.Data.EventOrLinkTargetPosition.CommitPosition);
			Assert.Equal(50, first.SafeTransactionFileReaderJoinPosition);
			Assert.Equal(100, second.SafeTransactionFileReaderJoinPosition);
			Assert.False(first.Data.IsJson);
			Assert.True(second.Data.IsJson);
		}

		[Fact]
		public void publishes_read_events_from_beginning_with_correct_next_event_number() {
			Assert.Equal(2, Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Count());
			Assert.Equal(
				"stream",
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last().EventStreamId);
			Assert.Equal(
				12, Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last().FromEventNumber);
		}

		[Fact]
		public void cannot_handle_repeated_read_events_completed() {
			var correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last()
				.CorrelationId;
			_edp.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					correlationId, "stream", 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								10, 50, Guid.NewGuid(), Guid.NewGuid(), 50, 0, "stream", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type", new byte[0], new byte[0]))
					}, null, false, "", 11, 10, true, 100));
			Assert.Equal(1, HandledMessages.OfType<ReaderSubscriptionMessage.Faulted>().Count());
		}

		[Fact]
		public void can_handle_following_read_events_completed() {
			var correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last()
				.CorrelationId;
			_edp.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					correlationId, "stream", 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								12, 250, Guid.NewGuid(), Guid.NewGuid(), 250, 0, "stream", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type", new byte[0], new byte[0]))
					}, null, false, "", 13, 11, true, 300));
		}
	}
}
