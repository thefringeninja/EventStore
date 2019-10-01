using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Services.TimerService;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;
using ReadStreamResult = EventStore.Core.Data.ReadStreamResult;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.event_reader.multi_stream_reader {
	public class when_has_been_created : TestFixtureWithExistingEvents {
		private MultiStreamEventReader _edp;
		private Guid _distibutionPointCorrelationId;
		private string[] _abStreams;
		private Dictionary<string, long> _ab12Tag;

		public when_has_been_created() {
			_ab12Tag = new Dictionary<string, long> {{"a", 1}, {"b", 2}};
			_abStreams = new[] {"a", "b"};

			_distibutionPointCorrelationId = Guid.NewGuid();
			_edp = new MultiStreamEventReader(
				_ioDispatcher, _bus, _distibutionPointCorrelationId, null, 0, _abStreams, _ab12Tag, false,
				new RealTimeProvider());
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
						_distibutionPointCorrelationId, "a", 100, 100, ReadStreamResult.Success, new ResolvedEvent[0],
						null, false, "", -1, 4, true, 100));
			});
		}
	}
}
