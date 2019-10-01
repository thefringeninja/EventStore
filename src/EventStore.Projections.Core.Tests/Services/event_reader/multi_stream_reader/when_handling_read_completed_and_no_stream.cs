using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.AwakeReaderService;
using EventStore.Core.Services.TimerService;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.event_reader.multi_stream_reader {
	public class when_handling_read_completed_and_no_stream : TestFixtureWithExistingEvents {
		private MultiStreamEventReader _edp;
		private Guid _distibutionPointCorrelationId;
		private Guid _firstEventId;
		private Guid _secondEventId;

		protected override void Given() {
			TicksAreHandledImmediately();
		}

		private string[] _abStreams;
		private Dictionary<string, long> _ab12Tag;

		public when_handling_read_completed_and_no_stream() {
			_ab12Tag = new Dictionary<string, long> {{"a", 1}, {"b", 0}};
			_abStreams = new[] {"a", "b"};

			_distibutionPointCorrelationId = Guid.NewGuid();
			_edp = new MultiStreamEventReader(
				_ioDispatcher, _bus, _distibutionPointCorrelationId, null, 0, _abStreams, _ab12Tag, false,
				new RealTimeProvider());
			_edp.Resume();
			_firstEventId = Guid.NewGuid();
			_secondEventId = Guid.NewGuid();
			var correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.Last(x => x.EventStreamId == "a").CorrelationId;
			_edp.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					correlationId, "a", 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								1, 50, Guid.NewGuid(), _firstEventId, 50, 0, "a", ExpectedVersion.Any, DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type1", new byte[] {1}, new byte[] {2})),
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								2, 100, Guid.NewGuid(), _secondEventId, 100, 0, "a", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type2", new byte[] {3}, new byte[] {4}))
					}, null, false, "", 3, 2, true, 200));
			correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.Last(x => x.EventStreamId == "b").CorrelationId;
			_edp.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					correlationId, "b", 100, 100, ReadStreamResult.Success, new ResolvedEvent[0], null, false, "",
					-1, ExpectedVersion.NoStream, true, 200));
		}

		[Fact]
		public void publishes_read_events_from_beginning_with_correct_next_event_number() {
			Assert.Equal(3, Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Count());
			Assert.True(
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
					.Any(m => m.EventStreamId == "a"));
			Assert.True(
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
					.Any(m => m.EventStreamId == "b"));
			Assert.Equal(
				3,
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
					.Last(m => m.EventStreamId == "a")
					.FromEventNumber);
			Assert.Equal(
				0,
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
					.Last(m => m.EventStreamId == "b")
					.FromEventNumber);
		}

		[Fact]
		public void publishes_correct_committed_event_received_messages() {
			Assert.Equal(
				3, Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Count());
			var first =
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().First();
			var second =
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>()
					.Skip(1)
					.First();
			var third =
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>()
					.Skip(2)
					.First();

			Assert.Null(third.Data);
			Assert.Equal(100, third.SafeTransactionFileReaderJoinPosition);

			Assert.Equal("event_type1", first.Data.EventType);
			Assert.Equal("event_type2", second.Data.EventType);
			Assert.Equal(_firstEventId, first.Data.EventId);
			Assert.Equal(_secondEventId, second.Data.EventId);
			Assert.Equal(1, first.Data.Data[0]);
			Assert.Equal(2, first.Data.Metadata[0]);
			Assert.Equal(3, second.Data.Data[0]);
			Assert.Equal(4, second.Data.Metadata[0]);
			Assert.Equal("a", first.Data.EventStreamId);
			Assert.Equal("a", second.Data.EventStreamId);
			Assert.Equal(50, first.Data.Position.PreparePosition);
			Assert.Equal(100, second.Data.Position.PreparePosition);
			Assert.Equal(-1, first.Data.Position.CommitPosition);
			Assert.Equal(-1, second.Data.Position.CommitPosition);
			Assert.Equal(50, first.SafeTransactionFileReaderJoinPosition);
			Assert.Equal(100, second.SafeTransactionFileReaderJoinPosition);
		}


		[Fact]
		public void publishes_subscribe_awake() {
			Assert.Equal(2, Consumer.HandledMessages.OfType<AwakeServiceMessage.SubscribeAwake>().Count());
		}

		[Fact]
		public void can_handle_following_read_events_completed() {
			_edp.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					_distibutionPointCorrelationId, "a", 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								3, 250, Guid.NewGuid(), Guid.NewGuid(), 250, 0, "a", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type", new byte[0], new byte[0]))
					}, null, false, "", 4, 3, true, 300));
		}
	}
}
