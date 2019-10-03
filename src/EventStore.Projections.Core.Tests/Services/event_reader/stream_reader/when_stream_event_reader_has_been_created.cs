using System;
using EventStore.Core.Messages;
using EventStore.Core.Services.TimerService;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using ReadStreamResult = EventStore.Core.Data.ReadStreamResult;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.event_reader.stream_reader {
	public class when_stream_event_reader_has_been_created : TestFixtureWithExistingEvents {
		private StreamEventReader _edp;

		//private Guid _publishWithCorrelationId;
		private Guid _distibutionPointCorrelationId;

		public when_stream_event_reader_has_been_created() {
			//_publishWithCorrelationId = Guid.NewGuid();
			_distibutionPointCorrelationId = Guid.NewGuid();
			_edp = new StreamEventReader(_bus, _distibutionPointCorrelationId, null, "stream", 0,
				new RealTimeProvider(), false,
				produceStreamDeletes: false);
		}

		[Fact]
		public void it_can_be_resumed() {
			_edp.Resume();
		}

		[Fact]
		public void it_cannot_be_paused() {
			Assert.Throws<InvalidOperationException>(() => { _edp.Pause(); });
		}

		[Fact]
		public void handle_read_events_completed_throws() {
			Assert.Throws<InvalidOperationException>(() => {
				_edp.Handle(
					new ClientMessage.ReadStreamEventsForwardCompleted(
						_distibutionPointCorrelationId, "stream", 100, 100, ReadStreamResult.Success,
						new ResolvedEvent[0], null, false, "", -1, 4, true, 100));
			});
		}
	}
}
