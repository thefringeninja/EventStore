using System;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage {
	public class when_caching_read_items {
		private readonly Guid _id = Guid.NewGuid();

		[Fact]
		public void the_item_can_be_read() {
			var cache = new DictionaryBasedCache();
			cache.PutRecord(12000, new PrepareLogRecord(12000, _id, _id, 12000, 0, "test", 1, DateTime.UtcNow,
				PrepareFlags.None, "type", new byte[0], new byte[0]));
			PrepareLogRecord read;
			Assert.True(cache.TryGetRecord(12000, out read));
			Assert.Equal(_id, read.EventId);
		}

		[Fact]
		public void cache_removes_oldest_item_when_max_count_reached() {
			var cache = new DictionaryBasedCache(9, 1024 * 1024 * 16);
			for (int i = 0; i < 10; i++)
				cache.PutRecord(i, new PrepareLogRecord(0, Guid.NewGuid(), _id, 0, 0, "test", 1, DateTime.UtcNow,
					PrepareFlags.None, "type", new byte[0], new byte[0]));
			PrepareLogRecord read;
			Assert.False(cache.TryGetRecord(0, out read));
		}

		[Fact]
		public void cache_removes_oldest_item_when_max_size_reached_by_data() {
			var cache = new DictionaryBasedCache(100, 1024 * 9);
			for (int i = 0; i < 10; i++)
				cache.PutRecord(i, new PrepareLogRecord(0, Guid.NewGuid(), _id, 0, 0, "test", 1, DateTime.UtcNow,
					PrepareFlags.None, "type", new byte[1024], new byte[0]));
			PrepareLogRecord read;
			Assert.False(cache.TryGetRecord(0, out read));
		}

		[Fact]
		public void cache_removes_oldest_item_when_max_size_reached_metadata() {
			var cache = new DictionaryBasedCache(100, 1024 * 9);
			for (int i = 0; i < 10; i++)
				cache.PutRecord(i, new PrepareLogRecord(0, Guid.NewGuid(), _id, 0, 0, "test", 1, DateTime.UtcNow,
					PrepareFlags.None, "type", new byte[0], new byte[1024]));
			PrepareLogRecord read;
			Assert.False(cache.TryGetRecord(0, out read));
		}

		[Fact]
		public void empty_cache_has_zeroed_statistics() {
			var cache = new DictionaryBasedCache(100, 1024 * 9);
			var stats = cache.GetStatistics();
			Assert.Equal(0, stats.MissCount);
			Assert.Equal(0, stats.HitCount);
			Assert.Equal(0, stats.Size);
			Assert.Equal(0, stats.Count);
		}

		[Fact]
		public void statistics_are_updated_with_hits() {
			var cache = new DictionaryBasedCache(100, 1024 * 9);
			cache.PutRecord(1, new PrepareLogRecord(1, Guid.NewGuid(), _id, 1, 0, "test", 1, DateTime.UtcNow,
				PrepareFlags.None, "type", new byte[0], new byte[1024]));
			PrepareLogRecord read;
			cache.TryGetRecord(1, out read);
			Assert.Equal(1, cache.GetStatistics().HitCount);
		}

		[Fact]
		public void statistics_are_updated_with_misses() {
			var cache = new DictionaryBasedCache(100, 1024 * 9);
			cache.PutRecord(1, new PrepareLogRecord(1, Guid.NewGuid(), _id, 1, 0, "test", 1, DateTime.UtcNow,
				PrepareFlags.None, "type", new byte[0], new byte[1024]));
			PrepareLogRecord read;
			cache.TryGetRecord(0, out read);
			Assert.Equal(1, cache.GetStatistics().MissCount);
		}

		[Fact]
		public void statistics_are_updated_with_total_count() {
			var cache = new DictionaryBasedCache(100, 1024 * 9);
			cache.PutRecord(1, new PrepareLogRecord(1, Guid.NewGuid(), _id, 1, 0, "test", 1, DateTime.UtcNow,
				PrepareFlags.None, "type", new byte[0], new byte[1024]));
			PrepareLogRecord read;
			cache.TryGetRecord(0, out read);
			Assert.Equal(1, cache.GetStatistics().Count);
		}

		[Fact]
		public void statistics_are_updated_with_total_size() {
			var cache = new DictionaryBasedCache(100, 1024 * 9);
			var record = new PrepareLogRecord(1, Guid.NewGuid(), _id, 1, 0, "test", 1, DateTime.UtcNow,
				PrepareFlags.None, "type", new byte[0], new byte[1024]);
			cache.PutRecord(1, record);
			PrepareLogRecord read;
			cache.TryGetRecord(0, out read);
			Assert.Equal(record.InMemorySize, cache.GetStatistics().Size);
		}
	}
}
