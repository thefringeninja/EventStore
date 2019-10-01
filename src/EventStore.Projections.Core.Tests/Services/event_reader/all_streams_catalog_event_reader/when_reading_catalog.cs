using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Services.UserManagement;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_reader.all_streams_catalog_event_reader {
	namespace when_reading_catalog {
        public abstract class with_all_streams_catalog_event_reader : TestFixtureWithEventReaderService {
			protected const int TailLength = 10;
			protected Guid _subscriptionId;
			protected IReaderStrategy _readerStrategy;
			protected ReaderSubscriptionOptions _readerSubscriptionOptions;

			protected override bool GivenHeadingReaderRunning() {
				return false;
			}

			protected override void Given() {
				base.Given();
				AllWritesSucceed();
				ExistingEvent("test-stream", "type1", "{}", "{Data: 1}");
				ExistingEvent("test-stream", "type1", "{}", "{Data: 2}");
				ExistingEvent("test-stream2", "type1", "{}", "{Data: 3}");

				ExistingEvent("test-stream2", "type1", "{}", "{Data: 4}");
				ExistingEvent("test-stream3", "type1", "{}", "{Data: 5}");
				ExistingEvent("test-stream3", "type1", "{}", "{Data: 6}");
				ExistingEvent("test-stream4", "type1", "{}", "{Data: 7}");

				ExistingEvent("$$test-stream", "$metadata", "", "{Meta: 1}");
				ExistingEvent("$$test-stream2", "$metadata", "", "{Meta: 2}");
				ExistingEvent("$$test-stream3", "$metadata", "", "{Meta: 3}");


				ExistingEvent("$streams", "$>", null, "0@test-stream");
				ExistingEvent("$streams", "$>", null, "0@test-stream2");
				ExistingEvent("$streams", "$>", null, "0@test-stream3");
				ExistingEvent("$streams", "$>", null, "0@test-stream4");
				NoOtherStreams();

				_subscriptionId = Guid.NewGuid();
				_readerStrategy = new ParallelQueryAllStreamsMasterReaderStrategy(
					"test",
					0,
					SystemAccount.Principal,
					_timeProvider);
				_readerSubscriptionOptions = new ReaderSubscriptionOptions(
					checkpointUnhandledBytesThreshold: 10000, checkpointProcessedEventsThreshold: 100,
					checkpointAfterMs: 10000,
					stopOnEof: true,
					stopAfterNEvents: null);
			}

			[Fact]
			public void returns_all_catalog_events() {
				var receivedEvents =
					Consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.CommittedEventReceived>().ToArray();

				Assert.Equal(4, receivedEvents.Length);
			}

			[Fact]
			public void events_are_correct() {
				var receivedEvents =
					Consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.CommittedEventReceived>().ToArray();
				var first = receivedEvents[0];
				var second = receivedEvents[1];
				var third = receivedEvents[2];
				var fourth = receivedEvents[3];

				Assert.Equal(true, first.Data.ResolvedLinkTo);
				Assert.Equal("{Meta: 1}", first.Data.StreamMetadata);
				Assert.Equal(true, second.Data.ResolvedLinkTo);
				Assert.Equal("{Meta: 2}", second.Data.StreamMetadata);
				Assert.Equal(true, third.Data.ResolvedLinkTo);
				Assert.Equal("{Meta: 3}", third.Data.StreamMetadata);
				Assert.Equal(true, fourth.Data.ResolvedLinkTo);
			}

			[Fact]
			public void returns_catalog_events_in_catalog_order() {
				var receivedEvents =
					Consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.CommittedEventReceived>().ToArray();

				Assert.True(
					(from e in receivedEvents
						orderby e.Data.Position
						select e.Data.Position)
					.SequenceEqual(from e in receivedEvents
						select e.Data.Position),
					"Incorrect event order received");
			}
		}

		public class when_starting_from_the_beginning : with_all_streams_catalog_event_reader {
			protected override IEnumerable<WhenStep> When() {
				var fromZeroPosition = CheckpointTag.FromByStreamPosition(0, "", -1, null, -1, 100000);
				yield return
					new ReaderSubscriptionManagement.Subscribe(
						_subscriptionId, fromZeroPosition, _readerStrategy, _readerSubscriptionOptions);
			}
		}
	}
}
