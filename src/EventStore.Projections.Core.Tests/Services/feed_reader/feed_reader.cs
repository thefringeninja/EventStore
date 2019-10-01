using System;
using System.Collections.Generic;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Services.UserManagement;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Projections.Core.EventReaders.Feeds;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.EventReaders.Feeds;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using System.Linq;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.feed_reader {
	namespace feed_reader {
		public class when_creating {
			private IPublisher _bus;

			private ReaderSubscriptionDispatcher _subscriptionDispatcher;

			private QuerySourcesDefinition _testQueryDefinition;

			public when_creating() {
				_bus = new InMemoryBus("test");
				_subscriptionDispatcher = new ReaderSubscriptionDispatcher(_bus);
				_testQueryDefinition = new QuerySourcesDefinition {AllStreams = true, AllEvents = true};
			}

			[Fact]
			public void it_can_be_created() {
				new FeedReader(
					_subscriptionDispatcher, SystemAccount.Principal, _testQueryDefinition,
					CheckpointTag.FromPosition(0, 0, -1), 10, Guid.NewGuid(), new NoopEnvelope(),
					new RealTimeProvider());
			}

			[Fact]
			public void null_subscription_dispatcher_throws_argument_null_exception() {
				Assert.Throws<ArgumentNullException>(() => {
					new FeedReader(
						null, SystemAccount.Principal, _testQueryDefinition, CheckpointTag.FromPosition(0, 0, -1), 10,
						Guid.NewGuid(), new NoopEnvelope(), new RealTimeProvider());
				});
			}

			[Fact]
			public void null_user_account_is_allowed() {
				new FeedReader(
					_subscriptionDispatcher, null, _testQueryDefinition, CheckpointTag.FromPosition(0, 0, -1), 10,
					Guid.NewGuid(), new NoopEnvelope(), new RealTimeProvider());
			}

			[Fact]
			public void null_query_definition_throws_argument_null_exception() {
				Assert.Throws<ArgumentNullException>(() => {
					new FeedReader(
						_subscriptionDispatcher, SystemAccount.Principal, null, CheckpointTag.FromPosition(0, 0, -1),
						10, Guid.NewGuid(),
						new NoopEnvelope(), new RealTimeProvider());
				});
			}

			[Fact]
			public void null_from_position_throws_argument_null_exception() {
				Assert.Throws<ArgumentNullException>(() => {
					new FeedReader(
						_subscriptionDispatcher, SystemAccount.Principal, _testQueryDefinition, null, 10,
						Guid.NewGuid(),
						new NoopEnvelope(), new RealTimeProvider());
				});
			}

			[Fact]
			public void null_envelope_throws_argument_null_exception() {
				Assert.Throws<ArgumentNullException>(() => {
					new FeedReader(
						_subscriptionDispatcher, SystemAccount.Principal, _testQueryDefinition,
						CheckpointTag.FromPosition(0, 0, -1), 10, Guid.NewGuid(), null, new RealTimeProvider());
				});
			}

			[Fact]
			public void zero_max_events_throws_argument_exception() {
				Assert.Throws<ArgumentException>(() => {
					new FeedReader(
						_subscriptionDispatcher, SystemAccount.Principal, _testQueryDefinition,
						CheckpointTag.FromPosition(0, 0, -1), 0, Guid.NewGuid(), new NoopEnvelope(),
						new RealTimeProvider());
				});
			}

			[Fact]
			public void negative_max_events_throws_argument_exception() {
				Assert.Throws<ArgumentException>(() => {
					new FeedReader(
						_subscriptionDispatcher, SystemAccount.Principal, _testQueryDefinition,
						CheckpointTag.FromPosition(0, 0, -1), -1, Guid.NewGuid(), new NoopEnvelope(),
						new RealTimeProvider());
				});
			}
		}

		public abstract class FeedReaderSpecification {
			protected InMemoryBus _bus;

			protected ReaderSubscriptionDispatcher _subscriptionDispatcher;

			protected QuerySourcesDefinition _testQueryDefinition;
			protected TestHandler<Message> _consumer;
			protected FeedReader _feedReader;

			public FeedReaderSpecification() {
				_bus = new InMemoryBus("test");
				_consumer = new TestHandler<Message>();
				_bus.Subscribe(_consumer);
				_subscriptionDispatcher = new ReaderSubscriptionDispatcher(_bus);
				_testQueryDefinition = GivenQuerySource();
				_feedReader = new FeedReader(
					_subscriptionDispatcher, SystemAccount.Principal, _testQueryDefinition, GivenFromPosition(), 10,
					Guid.NewGuid(), new PublishEnvelope(_bus), new RealTimeProvider());
				Given();
				When();
			}

			protected virtual void Given() {
			}

			protected abstract void When();

			protected virtual CheckpointTag GivenFromPosition() {
				return CheckpointTag.FromPosition(0, 0, -1);
			}

			protected abstract QuerySourcesDefinition GivenQuerySource();
		}

		public class when_starting : FeedReaderSpecification {
			protected override QuerySourcesDefinition GivenQuerySource() {
				return new QuerySourcesDefinition {AllStreams = true, AllEvents = true};
			}

			protected override void When() {
				_feedReader.Start();
			}

			[Fact]
			public void publishes_subscribe_message() {
				var subscribe = _consumer.HandledMessages.OfType<ReaderSubscriptionManagement.Subscribe>().ToArray();
				Assert.Equal(1, subscribe.Length);
			}

			[Fact]
			public void subscribes_to_a_finite_number_of_events() {
				var subscribe = _consumer.HandledMessages.OfType<ReaderSubscriptionManagement.Subscribe>().Single();
				Assert.NotNull(subscribe.Options);
				Assert.NotNull(subscribe.Options.StopAfterNEvents);
				Assert.True(0 < subscribe.Options.StopAfterNEvents);
				Assert.True(subscribe.Options.StopOnEof);
			}
		}

		public class when_handling_committed_events : FeedReaderSpecification {
			private Guid _subscriptionId;
			private int _number;

			protected override QuerySourcesDefinition GivenQuerySource() {
				return new QuerySourcesDefinition {AllStreams = true, AllEvents = true};
			}

			protected override void Given() {
				base.Given();
				_feedReader.Start();
				var subscribe = _consumer.HandledMessages.OfType<ReaderSubscriptionManagement.Subscribe>().Single();
				_subscriptionId = subscribe.SubscriptionId;
				_number = 0;
			}

			protected override void When() {
				for (var i = 0; i < 100; i++) {
					_feedReader.Handle(
						EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
							new ResolvedEvent(
								"stream", i, "stream", i, false, new TFPos(i * 100, i * 100 - 50),
								new TFPos(i * 100, i * 100 - 50), Guid.NewGuid(), "type", false, new byte[0],
								new byte[0],
								new byte[0], null, DateTime.UtcNow), _subscriptionId, _number++));
				}
			}

			[Fact]
			public void does_not_publish_feed_page_message() {
				var feedPageMessages = _consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().ToArray();
				Assert.Equal(0, feedPageMessages.Length);
			}
		}

		public class when_handling_eof_message : FeedReaderSpecification {
			private Guid _subscriptionId;
			private int _number;
			private int _maxN;

			protected override QuerySourcesDefinition GivenQuerySource() {
				return new QuerySourcesDefinition {AllStreams = true, AllEvents = true};
			}

			protected override void Given() {
				base.Given();
				_feedReader.Start();
				var subscribe = _consumer.HandledMessages.OfType<ReaderSubscriptionManagement.Subscribe>().Single();
				_subscriptionId = subscribe.SubscriptionId;
				_maxN = (int)subscribe.Options.StopAfterNEvents;
				_number = 0;
				for (var i = 0; i < _maxN; i++) {
					_feedReader.Handle(
						EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
							new ResolvedEvent(
								"stream", i, "stream", i, false, new TFPos(i * 100, i * 100 - 50),
								new TFPos(i * 100, i * 100 - 50), Guid.NewGuid(), "type", false, new byte[0],
								new byte[0],
								new byte[0], null, DateTime.UtcNow), _subscriptionId, _number++));
				}
			}

			protected override void When() {
				_feedReader.Handle(
					new EventReaderSubscriptionMessage.EofReached(
						_subscriptionId, CheckpointTag.FromPosition(0, _maxN * 100, _maxN * 100 - 50), _number++));
			}

			[Fact]
			public void publishes_feed_page_message() {
				var feedPageMessages = _consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().ToArray();
				Assert.Equal(1, feedPageMessages.Length);
			}

			[Fact]
			public void publishes_feed_page_with_all_received_events() {
				var feedPageMessage = _consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().Single();
				Assert.Equal(_maxN, feedPageMessage.Events.Length);
			}

			[Fact]
			public void publishes_feed_page_with_events_in_correct_order() {
				var feedPageMessage = _consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().Single();
				for (var i = 0; i < feedPageMessage.Events.Length - 1; i++) {
					var first = feedPageMessage.Events[i];
					var second = feedPageMessage.Events[i + 1];

					Assert.True(first.ReaderPosition < second.ReaderPosition);
					Assert.True(first.ResolvedEvent.Position < second.ResolvedEvent.Position);
				}
			}
		}

		public class when_reading_existing_events : TestFixtureWithFeedReaderService {
			private QuerySourcesDefinition _querySourcesDefinition;
			private CheckpointTag _fromPosition;
			private int _maxEvents;

			protected override void Given() {
				base.Given();
				ExistingEvent("test-stream", "type1", "{}", "{Data: 1}");
				ExistingEvent("test-stream", "type1", "{}", "{Data: 2}");
				ExistingEvent("test-stream", "type2", "{}", "{Data: 3}");

				_querySourcesDefinition = new QuerySourcesDefinition
					{Streams = new[] {"test-stream"}, AllEvents = true};
				_fromPosition = CheckpointTag.FromStreamPosition(0, "test-stream", -1);
				_maxEvents = 2;
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new FeedReaderMessage.ReadPage(
						Guid.NewGuid(), new PublishEnvelope(GetInputQueue()), SystemAccount.Principal,
						_querySourcesDefinition, _fromPosition, _maxEvents);
			}

			[Fact]
			public void publishes_feed_page_message() {
				var feedPage = Consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().ToArray();
				Assert.Equal(1, feedPage.Length);
			}

			[Fact]
			public void returns_correct_last_reader_position() {
				var feedPage = Consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().Single();
				Assert.Equal(CheckpointTag.FromStreamPosition(0, "test-stream", 1), feedPage.LastReaderPosition);
			}
		}

		public class when_reading_existing_events_by_parts : TestFixtureWithFeedReaderService {
			private QuerySourcesDefinition _querySourcesDefinition;
			private CheckpointTag _fromPosition;
			private int _maxEvents;

			protected override void Given() {
				base.Given();
				ExistingEvent("test-stream", "type1", "{}", "{Data: 1}");
				ExistingEvent("test-stream", "type1", "{}", "{Data: 2}");
				ExistingEvent("test-stream", "type2", "{}", "{Data: 3}");

				_querySourcesDefinition = new QuerySourcesDefinition
					{Streams = new[] {"test-stream"}, AllEvents = true};
				_fromPosition = CheckpointTag.FromStreamPosition(0, "test-stream", -1);
				_maxEvents = 2;
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new FeedReaderMessage.ReadPage(
						Guid.NewGuid(), new PublishEnvelope(GetInputQueue()), SystemAccount.Principal,
						_querySourcesDefinition, _fromPosition, _maxEvents);
				var feedPage = Consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().Single();
				yield return
					new FeedReaderMessage.ReadPage(
						Guid.NewGuid(), new PublishEnvelope(GetInputQueue()), SystemAccount.Principal,
						_querySourcesDefinition, feedPage.LastReaderPosition, _maxEvents);
			}

			[Fact]
			public void publishes_feed_page_message() {
				var feedPage = Consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().ToArray();
				Assert.Equal(2, feedPage.Length);
			}

			[Fact]
			public void returns_correct_last_reader_position() {
				var feedPage = Consumer.HandledMessages.OfType<FeedReaderMessage.FeedPage>().Last();
				Assert.Equal(CheckpointTag.FromStreamPosition(0, "test-stream", 2), feedPage.LastReaderPosition);
			}
		}
	}
}
