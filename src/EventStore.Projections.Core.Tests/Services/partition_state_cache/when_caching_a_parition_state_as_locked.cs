using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state_cache {
	public class when_caching_a_parition_state_as_locked {
		private PartitionStateCache _cache;
		private CheckpointTag _cachedAtCheckpointTag;

		public when_caching_a_parition_state_as_locked() {
			_cache = new PartitionStateCache();
			_cachedAtCheckpointTag = CheckpointTag.FromPosition(0, 1000, 900);
			_cache.CacheAndLockPartitionState("partition", new PartitionState("data", null, _cachedAtCheckpointTag),
				_cachedAtCheckpointTag);
		}

		[Fact]
		public void the_state_can_be_retrieved_as_locked() {
			var state = _cache.GetLockedPartitionState("partition");
			Assert.Equal("data", state.State);
		}

		[Fact]
		public void the_state_can_be_retrieved() {
			var state = _cache.TryGetPartitionState("partition");
			Assert.Equal("data", state.State);
		}

		[Fact]
		public void the_state_can_be_retrieved_as_unlocked_and_relocked_at_later_position() {
			var state = _cache.TryGetAndLockPartitionState("partition", CheckpointTag.FromPosition(0, 1500, 1400));
			Assert.Equal("data", state.State);
		}
	}
}
