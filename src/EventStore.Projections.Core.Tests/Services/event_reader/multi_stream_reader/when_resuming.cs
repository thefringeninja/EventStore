using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Services.TimerService;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using ReadStreamResult = EventStore.Core.Data.ReadStreamResult;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.event_reader.multi_stream_reader {
	public class when_resuming : TestFixtureWithExistingEvents {
		private MultiStreamEventReader _edp;
		private Guid _distibutionPointCorrelationId;

		private string[] _abStreams;
		private Dictionary<string, long> _ab12Tag;

		public when_resuming() {
			_ab12Tag = new Dictionary<string, long> {{"a", 1}, {"b", 2}};
			_abStreams = new[] {"a", "b"};

			_distibutionPointCorrelationId = Guid.NewGuid();
			_edp = new MultiStreamEventReader(
				_ioDispatcher, _bus, _distibutionPointCorrelationId, null, 0, _abStreams, _ab12Tag, false,
				new RealTimeProvider());

			_edp.Resume();
		}

		[Fact]
		public void it_cannot_be_resumed() {
			Assert.Throws<InvalidOperationException>(() => { _edp.Resume(); });
		}

		[Fact]
		public void it_cannot_be_paused() {
			_edp.Pause();
		}

		[Fact]
		public void it_publishes_read_events_from_beginning() {
			Assert.Equal(2, Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Count());
			Assert.True(
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
					.Any(m => m.EventStreamId == "a"));
			Assert.True(
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
					.Any(m => m.EventStreamId == "b"));
			Assert.Equal(
				1,
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
					.Single(m => m.EventStreamId == "a")
					.FromEventNumber);
			Assert.Equal(
				2,
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
					.Single(m => m.EventStreamId == "b")
					.FromEventNumber);
		}

		[Fact]
		public void can_handle_read_events_completed() {
			_edp.Handle(
				new ClientMessage.ReadStreamEventsForwardCompleted(
					_distibutionPointCorrelationId, "a", 100, 100, ReadStreamResult.Success,
					new[] {
						ResolvedEvent.ForUnresolvedEvent(
							new EventRecord(
								1, 50, Guid.NewGuid(), Guid.NewGuid(), 50, 0, "a", ExpectedVersion.Any,
								DateTime.UtcNow,
								PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
								"event_type", new byte[0], new byte[0]), 0)
					}, null, false, "", 2, 4, false, 100));
		}
	}
}
