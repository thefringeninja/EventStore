using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state_cache {
	public class when_the_partition_state_cache_has_been_created {
		private PartitionStateCache _cache;
		private Exception _exception;

		public when_the_partition_state_cache_has_been_created() {
			try {
				_cache = new PartitionStateCache();
			} catch (Exception ex) {
				_exception = ex;
			}
		}

		[Fact]
		public void it_has_been_created() {
			Assert.NotNull(_cache);
		}

		[Fact]
		public void state_can_be_cached() {
			CheckpointTag at = CheckpointTag.FromPosition(0, 100, 90);
			_cache.CacheAndLockPartitionState("partition", new PartitionState("data", null, at), at);
		}

		[Fact]
		public void no_items_are_cached() {
			Assert.Equal(0, _cache.CachedItemCount);
		}

		[Fact]
		public void random_item_cannot_be_retrieved_as_locked() {
			Assert.Null(
				_cache.TryGetAndLockPartitionState(
					"random", CheckpointTag.FromPosition(0, 200, 190)));
		}

		[Fact]
		public void random_item_cannot_be_retrieved() {
			Assert.Null(_cache.TryGetPartitionState("random"));
		}

		[Fact]
		public void root_partition_state_cannot_be_retrieved() {
			Assert.Null(
				_cache.TryGetAndLockPartitionState(
					"", CheckpointTag.FromPosition(0, 200, 190)));
		}

		[Fact]
		public void unlock_succeeds() {
			_cache.Unlock(CheckpointTag.FromPosition(0, 300, 290));
		}
	}
}
