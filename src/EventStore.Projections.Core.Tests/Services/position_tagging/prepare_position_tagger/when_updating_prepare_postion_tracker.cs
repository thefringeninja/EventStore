using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.prepare_position_tagger {
	public class when_updating_prepare_postion_tracker {
		private PositionTagger _tagger;
		private PositionTracker _positionTracker;

		public when_updating_prepare_postion_tracker() {
			// given
			_tagger = new PreparePositionTagger(0);
			_positionTracker = new PositionTracker(_tagger);
			var newTag = CheckpointTag.FromPreparePosition(0, 50);
			_positionTracker.UpdateByCheckpointTagInitial(newTag);
		}

		[Fact]
		public void checkpoint_tag_is_for_correct_position() {
			Assert.Equal(50, _positionTracker.LastTag.Position.PreparePosition);
		}

		[Fact]
		public void cannot_update_to_the_same_postion() {
			Assert.Throws<InvalidOperationException>(() => {
				var newTag = CheckpointTag.FromPreparePosition(0, 50);
				_positionTracker.UpdateByCheckpointTagForward(newTag);
			});
		}
	}
}
