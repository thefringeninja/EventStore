using System;
using System.Linq;
using EventStore.Core.Services.PersistentSubscription;
using Xunit;

namespace EventStore.Core.Tests.Services.PersistentSubscription {
	public class OutstandingMessageCacheTests {
		[Fact]
		public void when_created_has_zero_count() {
			var cache = new OutstandingMessageCache();
			Assert.Equal(0, cache.Count);
		}

		[Fact]
		public void can_remove_non_existing_item() {
			var cache = new OutstandingMessageCache();
			cache.Remove(Guid.NewGuid());
		}

		[Fact]
		public void adding_an_item_causes_count_to_go_up() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 0), 0),
				DateTime.Now);
			Assert.Equal(1, cache.Count);
			Assert.Equal(0, cache.GetLowestPosition());
		}

		[Fact]
		public void can_add_duplicate() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			var result1 =
				cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 0), 0),
					DateTime.Now);
			var result2 =
				cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 1), 0),
					DateTime.Now);
			Assert.Equal(1, cache.Count);
			Assert.Equal(0, cache.GetLowestPosition());
			Assert.Equal(StartMessageResult.Success, result1);
			Assert.Equal(StartMessageResult.SkippedDuplicate, result2);
		}

		[Fact]
		public void can_remove_duplicate() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 0), 0),
				DateTime.Now);
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 1), 0),
				DateTime.Now);
			cache.Remove(id);
			Assert.Equal(0, cache.Count);
			Assert.Equal(long.MaxValue, cache.GetLowestPosition());
		}

		[Fact]
		public void can_remove_existing_item() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 0), 0),
				DateTime.Now);
			cache.Remove(id);
			Assert.Equal(0, cache.Count);
			Assert.Equal(long.MaxValue, cache.GetLowestPosition());
		}

		[Fact]
		public void lowest_works_on_add() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 10), 0),
				DateTime.Now);
			Assert.Equal(10, cache.GetLowestPosition());
		}

		[Fact]
		public void lowest_works_on_adds_then_remove() {
			var id = Guid.NewGuid();
			var id2 = Guid.NewGuid();
			var id3 = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 10), 0),
				DateTime.Now);
			cache.StartMessage(new OutstandingMessage(id2, null, Helper.BuildFakeEvent(id2, "type", "name", 11), 0),
				DateTime.Now);
			cache.StartMessage(new OutstandingMessage(id3, null, Helper.BuildFakeEvent(id3, "type", "name", 12), 0),
				DateTime.Now);
			cache.Remove(id);
			Assert.Equal(11, cache.GetLowestPosition());
		}

		[Fact]
		public void lowest_on_empty_cache_returns_max() {
			var cache = new OutstandingMessageCache();
			Assert.Equal(long.MaxValue, cache.GetLowestPosition());
		}

		[Fact]
		public void lowest_ignores_replayed_events() {
			var cache = new OutstandingMessageCache();
			//normal event:
			var id1 = Guid.NewGuid();
			cache.StartMessage(new OutstandingMessage(id1, null, Helper.BuildFakeEvent(id1, "type", "name", 10), 0),
				DateTime.Now);
			//replayed event:
			var id2 = Guid.NewGuid();
			cache.StartMessage(new OutstandingMessage(id2, null, Helper.BuildFakeEvent(id2, "type", "$persistentsubscription-name::group-parked", 9), 0),
				DateTime.Now);
			Assert.Equal(10, cache.GetLowestPosition());
		}

		[Fact]
		public void get_expired_messages_returns_max_value_on_empty_cache() {
			var cache = new OutstandingMessageCache();
			Assert.Equal(0, cache.GetMessagesExpiringBefore(DateTime.Now).Count());
			Assert.Equal(long.MaxValue, cache.GetLowestPosition());
		}

		[Fact]
		public void message_that_expires_is_included_in_expired_list() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 0), 0),
				DateTime.Now.AddSeconds(-1));
			var expired = cache.GetMessagesExpiringBefore(DateTime.Now).ToList();
			Assert.Equal(1, expired.Count());
			Assert.Equal(id, expired.FirstOrDefault().EventId);
		}

		[Fact]
		public void message_that_expires_is_included_in_expired_list_with_another_that_should_not() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 0), 0),
				DateTime.Now.AddSeconds(-1));
			cache.StartMessage(
				new OutstandingMessage(Guid.NewGuid(), null, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "name", 1),
					0), DateTime.Now.AddSeconds(1));
			var expired = cache.GetMessagesExpiringBefore(DateTime.Now).ToList();
			Assert.Equal(1, expired.Count());
			Assert.Equal(id, expired.FirstOrDefault().EventId);
		}

		[Fact]
		public void message_that_is_removed_does_not_show_up_in_expired_list() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(
				new OutstandingMessage(id, null, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "name", 1), 0),
				DateTime.Now.AddSeconds(-11));
			cache.Remove(id);
			var expired = cache.WaitingTimeMessages();
			Assert.Equal(0, expired.Count());
		}

		[Fact]
		public void can_remove_non_first_message_and_have_removed_from_time() {
			var id1 = Guid.NewGuid();
			var id2 = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(
				new OutstandingMessage(id1, null, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "name", 1), 0),
				DateTime.Now.AddSeconds(-12));
			cache.StartMessage(
				new OutstandingMessage(id2, null, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "name", 2), 0),
				DateTime.Now.AddSeconds(-11));
			cache.Remove(id2);
			var expired = cache.WaitingTimeMessages();
			Assert.Equal(1, expired.Count());
			Assert.Equal(id1, expired.FirstOrDefault().Item2.MessageId);
		}

		[Fact]
		public void can_add_multiple_messages_same_time_different_ids() {
			var id1 = Guid.NewGuid();
			var id2 = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			var time = DateTime.Now.AddSeconds(-12);
			cache.StartMessage(
				new OutstandingMessage(id1, null, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "name", 1), 0), time);
			cache.StartMessage(
				new OutstandingMessage(id2, null, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "name", 2), 0), time);
			var expired = cache.WaitingTimeMessages();
			Assert.Equal(2, expired.Count());
		}

		[Fact]
		public void can_remove_second_message_same_time_different_ids() {
			var id1 = Guid.NewGuid();
			var id2 = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			var time = DateTime.Now.AddSeconds(-12);
			cache.StartMessage(
				new OutstandingMessage(id1, null, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "name", 1), 0), time);
			cache.StartMessage(
				new OutstandingMessage(id2, null, Helper.BuildFakeEvent(Guid.NewGuid(), "type", "name", 2), 0), time);
			cache.Remove(id2);
			var expired = cache.WaitingTimeMessages();
			Assert.Equal(id1, expired.FirstOrDefault().Item2.MessageId);
			Assert.Equal(1, expired.Count());
		}

		[Fact]
		public void message_that_notexpired_is_not_included_in_expired_list() {
			var id = Guid.NewGuid();
			var cache = new OutstandingMessageCache();
			cache.StartMessage(new OutstandingMessage(id, null, Helper.BuildFakeEvent(id, "type", "name", 0), 0),
				DateTime.Now.AddSeconds(1));
			var expired = cache.GetMessagesExpiringBefore(DateTime.Now).ToList();
			Assert.Equal(0, expired.Count());
		}
	}
}
