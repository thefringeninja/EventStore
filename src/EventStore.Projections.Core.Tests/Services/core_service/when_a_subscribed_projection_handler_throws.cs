using System;
using System.Linq;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using EventStore.Projections.Core.Tests.Services.event_reader.heading_event_reader;
using EventStore.Core.Data;

namespace EventStore.Projections.Core.Tests.Services.core_service {
	public class when_a_subscribed_projection_handler_throws : TestFixtureWithProjectionCoreService {
		public when_a_subscribed_projection_handler_throws() {
			var readerStrategy = new FakeReaderStrategy();
			var projectionCorrelationId = Guid.NewGuid();
			_readerService.Handle(
				new ReaderSubscriptionManagement.Subscribe(
					projectionCorrelationId, CheckpointTag.FromPosition(0, 0, 0), readerStrategy,
					new ReaderSubscriptionOptions(1000, 2000, 10000, false, stopAfterNEvents: null)));
			_readerService.Handle(
				ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
					readerStrategy.EventReaderId, new TFPos(20, 10), "throws", 10, false, Guid.NewGuid(),
					"type", false, new byte[0], new byte[0]));
		}

		[Fact]
		public void projection_is_notified_that_it_is_to_fault() {
			Assert.Equal(1, _consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.Failed>().Count());
		}
	}
}
