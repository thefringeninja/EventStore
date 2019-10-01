using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.stream_position_tagger {
	public class when_updating_postion_stream_position_tracker {
		private StreamPositionTagger _tagger;
		private PositionTracker _positionTracker;

		public when_updating_postion_stream_position_tracker() {
			// given
			_tagger = new StreamPositionTagger(0, "stream1");
			_positionTracker = new PositionTracker(_tagger);
			var newTag = CheckpointTag.FromStreamPosition(0, "stream1", 1);
			var newTag2 = CheckpointTag.FromStreamPosition(0, "stream1", 2);
			_positionTracker.UpdateByCheckpointTagInitial(newTag);
			_positionTracker.UpdateByCheckpointTagForward(newTag2);
		}

		[Fact]
		public void stream_position_is_updated() {
			Assert.Equal(2, _positionTracker.LastTag.Streams["stream1"]);
		}


		[Fact]
		public void cannot_update_to_the_same_postion() {
			Assert.Throws<InvalidOperationException>(() => {
				var newTag = CheckpointTag.FromStreamPosition(0, "stream1", 2);
				_positionTracker.UpdateByCheckpointTagForward(newTag);
			});
		}

		[Fact]
		public void it_cannot_be_updated_with_other_stream() {
			Assert.Throws<InvalidOperationException>(() => {
				// even not initialized (UpdateToZero can be removed)
				var newTag = CheckpointTag.FromStreamPosition(0, "other_stream1", 2);
				_positionTracker.UpdateByCheckpointTagForward(newTag);
			});
		}

		//TODO: write tests on updating with incompatible snapshot loaded
	}
}
