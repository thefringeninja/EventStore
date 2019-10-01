using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state_cache {
	public class when_relocking_the_state_at_later_position {
		private PartitionStateCache _cache;
		private CheckpointTag _cachedAtCheckpointTag;
		private PartitionState _relockedData;

		public when_relocking_the_state_at_later_position() {
			//given
			_cache = new PartitionStateCache();
			_cachedAtCheckpointTag = CheckpointTag.FromPosition(0, 1000, 900);
			_cache.CacheAndLockPartitionState("partition", new PartitionState("data", null, _cachedAtCheckpointTag),
				_cachedAtCheckpointTag);
			_relockedData = _cache.TryGetAndLockPartitionState("partition", CheckpointTag.FromPosition(0, 2000, 1900));
		}

		[Fact]
		public void returns_correct_cached_data() {
			Assert.Equal("data", _relockedData.State);
		}

		[Fact]
		public void relocked_state_can_be_retrieved_as_locked() {
			var state = _cache.GetLockedPartitionState("partition");
			Assert.Equal("data", state.State);
		}

		[Fact]
		public void cannot_be_relocked_at_the_previous_position() {
			Assert.Throws<InvalidOperationException>(() => {
				_cache.TryGetAndLockPartitionState("partition", _cachedAtCheckpointTag);
			});
		}

		[Fact]
		public void the_state_can_be_retrieved() {
			var state = _cache.TryGetPartitionState("partition");
			Assert.Equal("data", state.State);
		}
	}
}
