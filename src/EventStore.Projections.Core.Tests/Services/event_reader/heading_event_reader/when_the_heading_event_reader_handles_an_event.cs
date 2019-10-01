using System;
using EventStore.Core.Data;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Tests.Helpers;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.projections_manager.managed_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_reader.heading_event_reader {
	public class when_the_heading_event_reader_handles_an_event : TestFixtureWithReadWriteDispatchers {
		private HeadingEventReader _point;
		private Exception _exception;
		private Guid _distibutionPointCorrelationId;

		public when_the_heading_event_reader_handles_an_event() {
			_point = new HeadingEventReader(10, _bus);

			_distibutionPointCorrelationId = Guid.NewGuid();
			_point.Start(
				_distibutionPointCorrelationId,
				new TransactionFileEventReader(_bus, _distibutionPointCorrelationId, null, new TFPos(0, -1),
					new RealTimeProvider()));
			_point.Handle(
				ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
					_distibutionPointCorrelationId, new TFPos(20, 10), "stream", 10, false, Guid.NewGuid(),
					"type", false, new byte[0], new byte[0]));
		}

		[Fact]
		public void can_handle_next_event() {
			_point.Handle(
				ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
					_distibutionPointCorrelationId, new TFPos(40, 30), "stream", 12, false, Guid.NewGuid(),
					"type", false, new byte[0], new byte[0]));
		}

		//TODO: SW1
/*
        [Fact]
        public void can_handle_special_update_position_event()
        {
            _point.Handle(
                new ProjectionCoreServiceMessage.CommittedEventDistributed(
                    _distibutionPointCorrelationId, new EventPosition(long.MinValue, 30), "stream", 12, false, null));
        }
*/

		[Fact]
		public void cannot_handle_previous_event() {
			Assert.Throws<InvalidOperationException>(() => {
				_point.Handle(
					ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
						_distibutionPointCorrelationId, new TFPos(5, 0), "stream", 8, false, Guid.NewGuid(), "type",
						false, new byte[0], new byte[0]));
			});
		}

		[Fact]
		public void a_projection_can_be_subscribed_after_event_position() {
			var subscribed = _point.TrySubscribe(Guid.NewGuid(), new FakeReaderSubscription(), 30);
			Assert.Equal(true, subscribed);
		}

		[Fact]
		public void a_projection_cannot_be_subscribed_at_earlier_position() {
			var subscribed = _point.TrySubscribe(Guid.NewGuid(), new FakeReaderSubscription(), 10);
			Assert.False(subscribed);
		}
	}
}
