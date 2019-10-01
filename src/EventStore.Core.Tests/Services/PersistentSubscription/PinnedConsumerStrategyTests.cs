using System;
using EventStore.Core.Index.Hashes;
using EventStore.Core.Services.PersistentSubscription;
using EventStore.Core.Services.PersistentSubscription.ConsumerStrategy;
using EventStore.Core.Tests.Services.Replication;
using EventStore.Core.Tests.Services.Storage;
using Xunit;

namespace EventStore.Core.Tests.Services.PersistentSubscription {
	public class PinnedConsumerStrategyTests {
		[Fact]
		public void live_subscription_with_pinned_pushes_events_to_same_client_for_stream_id() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();

			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor("$ce-streamName", "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
					.StartFromCurrent());
			reader.Load(null);
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client2Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 1));

			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(0, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 2));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 3));

			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(2, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 4));

			Assert.Equal(3, client1Envelope.Replies.Count);
			Assert.Equal(2, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-3", 5));

			Assert.Equal(4, client1Envelope.Replies.Count);
			Assert.Equal(2, client2Envelope.Replies.Count);
		}

		[Fact]
		public void new_stream_is_round_robinned_to_next_consumer() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor("$ce-streamName", "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
					.StartFromCurrent());
			reader.Load(null);
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client2Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0));

			Assert.Equal(1, client1Envelope.Replies.Count);
			Assert.Equal(0, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 1));

			Assert.Equal(1, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-3", 2));

			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-4", 4));

			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(2, client2Envelope.Replies.Count);
		}

		[Fact]
		public void resolved_link_events_use_event_stream_id() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor(subsctiptionStream, "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
					.StartFromCurrent());
			reader.Load(null);
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client2Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 0,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0)));

			Assert.Equal(1, client1Envelope.Replies.Count);
			Assert.Equal(0, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 1,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 0)));

			Assert.Equal(1, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 2,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-3", 0)));

			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 3,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-4", 0)));

			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(2, client2Envelope.Replies.Count);
		}

		[Fact]
		public void link_events_use_data_to_detect_stream_id() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor(subsctiptionStream, "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
					.StartFromCurrent());
			reader.Load(null);
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client2Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 0,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0), false));

			Assert.Equal(1, client1Envelope.Replies.Count);
			Assert.Equal(0, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 1,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 0), false));

			Assert.Equal(1, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 2,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-3", 0), false));

			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 3,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-4", 0), false));

			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(2, client2Envelope.Replies.Count);
		}

		[Fact]
		public void unsubscribed_client_has_assigned_streams_sent_to_another_client() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor(subsctiptionStream, "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
					.StartFromCurrent());
			reader.Load(null);
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
			var client2Id = Guid.NewGuid();
			sub.AddClient(client2Id, Guid.NewGuid(), client2Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 0,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0), false));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 1,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 0), false));

			Assert.Equal(1, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);

			sub.RemoveClientByCorrelationId(client2Id, false);

			// Message 2 should be retried on client 1 as it wasn't acked.
			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);
		}

		[Fact]
		public void new_subscription_gets_work() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var client3Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor(subsctiptionStream, "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new ByLengthHasher()))
					.StartFromCurrent());
			reader.Load(null);
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client2Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "1", 0));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "11", 1));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "111", 2));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "1111", 3));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "11111", 4));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "111111", 5));

			Assert.Equal(3, client1Envelope.Replies.Count);
			Assert.Equal(3, client2Envelope.Replies.Count);

			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client3Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "1", 6));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "11", 7));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "111", 8));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "1111", 9));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "11111", 10));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "111111", 11));

			Assert.Equal(5, client1Envelope.Replies.Count);
			Assert.Equal(5, client2Envelope.Replies.Count);
			Assert.Equal(2, client3Envelope.Replies.Count);
		}

		[Fact]
		public void bad_availablecapacity_scneanrio_causing_null_reference() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor(subsctiptionStream, "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new ByLengthHasher()))
					.StartFromCurrent());
			reader.Load(null);
			var conn1Id = Guid.NewGuid();
			sub.AddClient(Guid.NewGuid(), conn1Id, client1Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "1", 0));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "11", 1));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildFakeEvent(Guid.NewGuid(), "type", "111", 2));

			Assert.Equal(3, client1Envelope.Replies.Count);

			var conn2Id = Guid.NewGuid();
			sub.AddClient(Guid.NewGuid(), conn2Id, client2Envelope, 10, "foo", "bar");

			Assert.Equal(3, client1Envelope.Replies.Count);
			Assert.Equal(0, client2Envelope.Replies.Count);

			sub.RemoveClientByConnectionId(conn1Id);

			// Used to throw a null reference exception.
			sub.RemoveClientByConnectionId(conn2Id);
		}

		[Fact]
		public void disconnected_client_has_assigned_streams_sent_to_another_client() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor(subsctiptionStream, "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
					.StartFromCurrent());
			reader.Load(null);
			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client1Envelope, 10, "foo", "bar");
			var client2Id = Guid.NewGuid();
			sub.AddClient(Guid.NewGuid(), client2Id, client2Envelope, 10, "foo", "bar");

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 0,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0), false));
			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 1,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 0), false));

			Assert.Equal(1, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);

			sub.RemoveClientByConnectionId(client2Id);

			// Message 2 should be retried on client 1 as it wasn't acked.
			Assert.Equal(2, client1Envelope.Replies.Count);
			Assert.Equal(1, client2Envelope.Replies.Count);
		}

		[Fact]
		public void available_capacity_is_tracked() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			PersistentSubscriptionParams settings = PersistentSubscriptionParamsBuilder
				.CreateFor(subsctiptionStream, "groupName")
				.WithEventLoader(new FakeStreamReader(x => { }))
				.WithCheckpointReader(reader)
				.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
				.WithMessageParker(new FakeMessageParker())
				.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
				.StartFromCurrent();
			var consumerStrategy = (PinnedPersistentSubscriptionConsumerStrategy)settings.ConsumerStrategy;
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(settings);
			reader.Load(null);
			var client1Id = Guid.NewGuid();
			sub.AddClient(Guid.NewGuid(), client1Id, client1Envelope, 14, "foo", "bar");

			Assert.Equal(consumerStrategy.AvailableCapacity, 14);

			var client2Id = Guid.NewGuid();
			sub.AddClient(Guid.NewGuid(), client2Id, client2Envelope, 10, "foo", "bar");

			Assert.Equal(consumerStrategy.AvailableCapacity, 24);

			sub.RemoveClientByConnectionId(client2Id);

			Assert.Equal(consumerStrategy.AvailableCapacity, 14);

			sub.RemoveClientByConnectionId(client1Id);

			Assert.Equal(consumerStrategy.AvailableCapacity, 0);
		}

		[Fact]
		public void available_capacity_is_tracked_with_inflight_messages() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			PersistentSubscriptionParams settings = PersistentSubscriptionParamsBuilder
				.CreateFor(subsctiptionStream, "groupName")
				.WithEventLoader(new FakeStreamReader(x => { }))
				.WithCheckpointReader(reader)
				.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
				.WithMessageParker(new FakeMessageParker())
				.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
				.StartFromCurrent();
			var consumerStrategy = (PinnedPersistentSubscriptionConsumerStrategy)settings.ConsumerStrategy;
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(settings);
			reader.Load(null);
			var client1Id = Guid.NewGuid();
			sub.AddClient(Guid.NewGuid(), client1Id, client1Envelope, 14, "foo", "bar");
			Assert.Equal(consumerStrategy.AvailableCapacity, 14);

			var client2Id = Guid.NewGuid();
			sub.AddClient(Guid.NewGuid(), client2Id, client2Envelope, 10, "foo", "bar");

			Assert.Equal(consumerStrategy.AvailableCapacity, 24);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 0,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0), false));

			Assert.Equal(consumerStrategy.AvailableCapacity, 23);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 1,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 0), false));

			Assert.Equal(consumerStrategy.AvailableCapacity, 22);

			sub.RemoveClientByConnectionId(client2Id);

			Assert.Equal(consumerStrategy.AvailableCapacity, 12);

			sub.RemoveClientByConnectionId(client1Id);

			Assert.Equal(consumerStrategy.AvailableCapacity, 0);
		}

		[Fact]
		public void available_capacity_is_tracked_with_acked_messages() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			PersistentSubscriptionParams settings = PersistentSubscriptionParamsBuilder
				.CreateFor(subsctiptionStream, "groupName")
				.WithEventLoader(new FakeStreamReader(x => { }))
				.WithCheckpointReader(reader)
				.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
				.WithMessageParker(new FakeMessageParker())
				.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
				.StartFromCurrent();
			var consumerStrategy = (PinnedPersistentSubscriptionConsumerStrategy)settings.ConsumerStrategy;
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(settings);
			reader.Load(null);
			var correlationId1 = Guid.NewGuid();
			sub.AddClient(correlationId1, Guid.NewGuid(), client1Envelope, 14, "foo", "bar");
			Assert.Equal(consumerStrategy.AvailableCapacity, 14);

			var correlationId2 = Guid.NewGuid();
			sub.AddClient(correlationId2, Guid.NewGuid(), client2Envelope, 10, "foo", "bar");

			Assert.Equal(consumerStrategy.AvailableCapacity, 24);

			var message1 = Guid.NewGuid();
			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(message1, subsctiptionStream, 0,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0), false));

			Assert.Equal(consumerStrategy.AvailableCapacity, 23);

			var message2 = Guid.NewGuid();
			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(message2, subsctiptionStream, 1,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 0), false));

			Assert.Equal(consumerStrategy.AvailableCapacity, 22);

			sub.AcknowledgeMessagesProcessed(correlationId1, new[] {message1});

			Assert.Equal(consumerStrategy.AvailableCapacity, 23);

			sub.AcknowledgeMessagesProcessed(correlationId2, new[] {message2});

			Assert.Equal(consumerStrategy.AvailableCapacity, 24);
		}

		[Fact]
		public void events_are_skipped_if_assigned_client_full() {
			var client1Envelope = new FakeEnvelope();
			var client2Envelope = new FakeEnvelope();
			var reader = new FakeCheckpointReader();
			const string subsctiptionStream = "$ce-streamName";
			var sub = new Core.Services.PersistentSubscription.PersistentSubscription(
				PersistentSubscriptionParamsBuilder.CreateFor(subsctiptionStream, "groupName")
					.WithEventLoader(new FakeStreamReader(x => { }))
					.WithCheckpointReader(reader)
					.WithCheckpointWriter(new FakeCheckpointWriter(x => { }))
					.WithMessageParker(new FakeMessageParker())
					.CustomConsumerStrategy(new PinnedPersistentSubscriptionConsumerStrategy(new XXHashUnsafe()))
					.StartFromCurrent());
			reader.Load(null);

			var correlationId = Guid.NewGuid();
			sub.AddClient(correlationId, Guid.NewGuid(), client1Envelope, 1, "foo", "bar");

			sub.AddClient(Guid.NewGuid(), Guid.NewGuid(), client2Envelope, 1, "foo", "bar");


			var message1 = Guid.NewGuid();
			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(message1, subsctiptionStream, 0,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0), false));

			Assert.Equal(client1Envelope.Replies.Count, 1);
			Assert.Equal(client2Envelope.Replies.Count, 0);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 1,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-1", 0), false));

			Assert.Equal(client1Envelope.Replies.Count, 1);
			Assert.Equal(client2Envelope.Replies.Count, 0);
			Assert.Equal(sub.StreamBuffer.BufferCount, 1);

			sub.NotifyLiveSubscriptionMessage(Helper.BuildLinkEvent(Guid.NewGuid(), subsctiptionStream, 2,
				Helper.BuildFakeEvent(Guid.NewGuid(), "type", "streamName-2", 0), false));

			Assert.Equal(client1Envelope.Replies.Count, 1);
			Assert.Equal(client2Envelope.Replies.Count, 1);

			Assert.Equal(sub.StreamBuffer.BufferCount, 1);

			sub.AcknowledgeMessagesProcessed(correlationId, new[] {message1});

			Assert.Equal(client1Envelope.Replies.Count, 2);
			Assert.Equal(client2Envelope.Replies.Count, 1);

			Assert.Equal(sub.StreamBuffer.BufferCount, 0);
		}
	}
}
