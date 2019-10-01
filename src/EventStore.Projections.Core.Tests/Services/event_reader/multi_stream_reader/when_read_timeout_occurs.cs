using System;
using System.Collections.Generic;
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

namespace EventStore.Projections.Core.Tests.Services.event_reader.multi_stream_reader {
	public class when_read_timeout_occurs : TestFixtureWithExistingEvents {
		private MultiStreamEventReader _eventReader;
		private Guid _distibutionPointCorrelationId;
		private Guid _streamReadACorrelationId;
		private Guid _streamReadBCorrelationId;

		protected override void Given() {
			TicksAreHandledImmediately();
		}

		private string[] _abStreams;
		private Dictionary<string, long> _ab12Tag;

		public when_read_timeout_occurs() {
			_ab12Tag = new Dictionary<string, long> {{"a", 1}, {"b", 2}};
			_abStreams = new[] {"a", "b"};

			_distibutionPointCorrelationId = Guid.NewGuid();
			_eventReader = new MultiStreamEventReader(
				_ioDispatcher, _bus, _distibutionPointCorrelationId, null, 0, _abStreams, _ab12Tag, false,
				new RealTimeProvider());
			_eventReader.Resume();
			_streamReadACorrelationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.Last(x => x.EventStreamId == "a").CorrelationId;
			_eventReader.Handle(
				new ProjectionManagementMessage.Internal.ReadTimeout(_streamReadACorrelationId, "a"));
			_eventReader.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					_streamReadACorrelationId, "a", 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								1, 50, Guid.NewGuid(), Guid.NewGuid(), 50, 0, "a", ExpectedVersion.Any, DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd |
								PrepareFlags.IsJson,
								"event_type1", new byte[] {1}, new byte[] {2})),
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								2, 150, Guid.NewGuid(), Guid.NewGuid(), 150, 0, "a", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type2", new byte[] {3}, new byte[] {4}))
					}, null, false, "", 3, 2, true, 200));
			_streamReadBCorrelationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.Last(x => x.EventStreamId == "b").CorrelationId;
			_eventReader.Handle(
				new ProjectionManagementMessage.Internal.ReadTimeout(_streamReadBCorrelationId, "b"));
			_eventReader.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					_streamReadBCorrelationId, "b", 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								2, 100, Guid.NewGuid(), Guid.NewGuid(), 100, 0, "b", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type1", new byte[] {1}, new byte[] {2})),
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								3, 200, Guid.NewGuid(), Guid.NewGuid(), 200, 0, "b", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type2", new byte[] {3}, new byte[] {4}))
					}, null, false, "", 4, 3, true, 200));
		}

		[Fact]
		public void should_not_deliver_events() {
			Assert.Equal(0,
				Consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Count());
		}

		[Fact]
		public void should_attempt_another_read_for_the_timed_out_reads() {
			var streamAReads = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.Where(x => x.EventStreamId == "a");

			Assert.Equal(streamAReads.First().CorrelationId, _streamReadACorrelationId);
			Assert.Equal(1, streamAReads.Skip(1).Count());

			var streamBReads = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.Where(x => x.EventStreamId == "b");

			Assert.Equal(streamBReads.First().CorrelationId, _streamReadBCorrelationId);
			Assert.Equal(1, streamBReads.Skip(1).Count());
		}
	}
}
