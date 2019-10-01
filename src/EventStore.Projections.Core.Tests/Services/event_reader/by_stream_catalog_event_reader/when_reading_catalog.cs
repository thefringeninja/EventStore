using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_reader.by_stream_catalog_event_reader {
	namespace when_reading_catalog {
			public abstract class with_catalog_stream : TestFixtureWithEventReaderService {
			protected const int TailLength = 10;
			protected Guid _subscriptionId;
			private QuerySourcesDefinition _sourceDefinition;
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

				ExistingEvent("catalog", "$>", null, "0@test-stream");
				ExistingEvent("catalog", "$>", null, "0@test-stream2");
				ExistingEvent("catalog", "$>", null, "0@test-stream3");

				_subscriptionId = Guid.NewGuid();
				_sourceDefinition = new QuerySourcesDefinition {
					CatalogStream = "catalog",
					AllEvents = true,
					ByStreams = true,
					Options = new QuerySourcesDefinitionOptions { }
				};
				_readerStrategy = ReaderStrategy.Create(
					"test",
					0,
					_sourceDefinition,
					_timeProvider,
					stopOnEof: true,
					runAs: null);
				_readerSubscriptionOptions = new ReaderSubscriptionOptions(
					checkpointUnhandledBytesThreshold: 10000, checkpointProcessedEventsThreshold: 100,
					checkpointAfterMs: 10000, stopOnEof: true,
					stopAfterNEvents: null);
			}

			[Fact]
			public void returns_all_events() {
				var receivedEvents =
					Consumer.HandledMessages.OfType<EventReaderSubscriptionMessage.CommittedEventReceived>().ToArray();

				Assert.Equal(6, receivedEvents.Length);
			}

			[Fact]
			public void returns_events_in_catalog_order() {
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

			[Fact]
			public void publishes_partition_eof_on_each_stream_eof() {
				var messages =
					HandledMessages.Where(
						v =>
							v is EventReaderSubscriptionMessage.CommittedEventReceived
							|| v is EventReaderSubscriptionMessage.PartitionEofReached).ToList();

				Assert.Equal(9, messages.Count);
				Assert.IsAssignableFrom<EventReaderSubscriptionMessage.PartitionEofReached>(messages[2]);
				Assert.Equal("test-stream",
					((EventReaderSubscriptionMessage.PartitionEofReached)messages[2]).Partition);
				Assert.IsAssignableFrom<EventReaderSubscriptionMessage.PartitionEofReached>(messages[5]);
				Assert.Equal("test-stream2",
					((EventReaderSubscriptionMessage.PartitionEofReached)messages[5]).Partition);
				Assert.IsAssignableFrom<EventReaderSubscriptionMessage.PartitionEofReached>(messages[8]);
				Assert.Equal("test-stream3",
					((EventReaderSubscriptionMessage.PartitionEofReached)messages[8]).Partition);
			}
		}

		public class when_starting_from_the_beginning : with_catalog_stream {
			protected override IEnumerable<WhenStep> When() {
				var fromZeroPosition = CheckpointTag.FromByStreamPosition(0, "catalog", -1, null, -1, 1000);
				yield return
					new ReaderSubscriptionManagement.Subscribe(
						_subscriptionId, fromZeroPosition, _readerStrategy, _readerSubscriptionOptions);
			}
		}

		public class when_new_events_appear_after_subscribing : with_catalog_stream {
			protected override IEnumerable<WhenStep> When() {
				var fromZeroPosition = CheckpointTag.FromByStreamPosition(0, "catalog", -1, null, -1, 1000);
				yield return
					new WhenStep(
						new ReaderSubscriptionManagement.Subscribe(
							_subscriptionId, fromZeroPosition, _readerStrategy, _readerSubscriptionOptions),
						CreateWriteEvent("test-stream2", "type1", "{Data: 8}"),
						CreateWriteEvent("catalog", "$>", "2@test-stream2"));
			}
		}

		public class when_new_streams_appear_after_subscribing : with_catalog_stream {
			protected override IEnumerable<WhenStep> When() {
				var fromZeroPosition = CheckpointTag.FromByStreamPosition(0, "catalog", -1, null, -1, 1000);
				yield return
					new WhenStep(
						new ReaderSubscriptionManagement.Subscribe(
							_subscriptionId, fromZeroPosition, _readerStrategy, _readerSubscriptionOptions),
						CreateWriteEvent("test-stream4", "type1", "{Data: 8}"),
						CreateWriteEvent("catalog", "$>", "0@test-stream4"));
			}
		}
	}
}
