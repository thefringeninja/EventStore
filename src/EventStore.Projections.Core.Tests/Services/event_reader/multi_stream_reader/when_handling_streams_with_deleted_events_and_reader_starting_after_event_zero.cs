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
	public class
		when_handling_streams_with_deleted_events_and_reader_starting_after_event_zero : TestFixtureWithExistingEvents {
		private MultiStreamEventReader _edp;
		private int _fromSequenceNumber;
		private string[] _streamNames;
		private Dictionary<string, long> _streamPositions;
		private Guid _distibutionPointCorrelationId;

		protected override void Given() {
			TicksAreHandledImmediately();
			_fromSequenceNumber = 10;
			_streamNames = new[] {"stream1", "stream2"};
			_streamPositions = new Dictionary<string, long> {{"stream1", _fromSequenceNumber}, {"stream2", 100}};
		}

		public when_handling_streams_with_deleted_events_and_reader_starting_after_event_zero() {
			_distibutionPointCorrelationId = Guid.NewGuid();

			_edp = new MultiStreamEventReader(
				_ioDispatcher, _bus, _distibutionPointCorrelationId, null, 0, _streamNames, _streamPositions, false,
				new RealTimeProvider());

			_edp.Resume();
		}

		private void HandleEvents(string stream, long[] eventNumbers) {
			string eventType = "event_type";
			List<ResolvedEvent> events = new List<ResolvedEvent>();

			foreach (long eventNumber in eventNumbers) {
				events.Add(
					ResolvedEvent.ForUnresolvedEvent(
						new EventRecord(
							eventNumber, 50 * (eventNumber + 1), Guid.NewGuid(), Guid.NewGuid(), 50 * (eventNumber + 1),
							0, stream, ExpectedVersion.Any, DateTime.UtcNow,
							PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
							eventType, new byte[] {0}, new byte[] {0}
						)
					)
				);
			}

			long start, end;
			if (eventNumbers.Length > 0) {
				start = eventNumbers[0];
				end = eventNumbers[eventNumbers.Length - 1];
			} else {
				start = _fromSequenceNumber;
				end = _fromSequenceNumber;
			}

			var correlationId = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.Last(x => x.EventStreamId == stream).CorrelationId;

			_edp.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					correlationId, stream, start, 100, ReadStreamResult.Success, events.ToArray(), null, false, "",
					start + 1, end, true, 200)
			);
		}

		private void HandleEvents(string stream, long start, long end) {
			List<long> eventNumbers = new List<long>();
			for (long i = start; i <= end; i++) eventNumbers.Add(i);
			HandleEvents(stream, eventNumbers.ToArray());
		}

		[Fact]
		public void allows_first_event_to_be_equal_to_sequence_number() {
			long eventSequenceNumber = _fromSequenceNumber;

			HandleEvents(_streamNames[0], eventSequenceNumber, eventSequenceNumber);
			//to trigger event delivery:
			HandleEvents(_streamNames[1], 100, 101);
		}

		[Fact]
		public void should_not_allow_first_event_to_be_greater_than_sequence_number() {
			long eventSequenceNumber = _fromSequenceNumber + 5;

			HandleEvents(_streamNames[0], eventSequenceNumber, eventSequenceNumber);
			//to trigger event delivery:
			HandleEvents(_streamNames[1], 100, 101);

			Assert.Equal(1, HandledMessages.OfType<ReaderSubscriptionMessage.Faulted>().Count());
		}

		[Fact]
		public void should_not_allow_first_event_to_be_less_than_sequence_number() {
			long eventSequenceNumber = _fromSequenceNumber - 1;

			HandleEvents(_streamNames[0], eventSequenceNumber, eventSequenceNumber);
			//to trigger event delivery:
			HandleEvents(_streamNames[1], 100, 101);

			Assert.Equal(1, HandledMessages.OfType<ReaderSubscriptionMessage.Faulted>().Count());
		}

		[Fact]
		public void events_after_first_event_should_not_be_in_sequence() {
			//_fromSequenceNumber+2 has been omitted
			HandleEvents(_streamNames[0],
				new long[] {
					_fromSequenceNumber, _fromSequenceNumber + 1, _fromSequenceNumber + 3, _fromSequenceNumber + 4
				});
			//to trigger event delivery:
			HandleEvents(_streamNames[1], 100, 101);

			Assert.Equal(2, HandledMessages.OfType<ReaderSubscriptionMessage.Faulted>().Count());
		}
	}
}
