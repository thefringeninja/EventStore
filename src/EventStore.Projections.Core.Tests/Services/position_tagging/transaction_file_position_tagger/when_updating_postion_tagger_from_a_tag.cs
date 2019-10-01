using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.transaction_file_position_tagger {
	public class when_updating_postion_tagger_from_a_tag {
		private PositionTagger _tagger;
		private CheckpointTag _tag;
		private PositionTracker _positionTracker;

		public when_updating_postion_tagger_from_a_tag() {
			// given
			var tagger = new TransactionFilePositionTagger(0);
			var positionTracker = new PositionTracker(tagger);

			var newTag = CheckpointTag.FromPosition(0, 100, 50);
			positionTracker.UpdateByCheckpointTagInitial(newTag);
			_tag = positionTracker.LastTag;
			_tagger = new TransactionFilePositionTagger(0);
			_positionTracker = new PositionTracker(_tagger);
			// when 

			_positionTracker.UpdateByCheckpointTagInitial(_tag);
		}

		[Fact]
		public void position_is_updated() {
			Assert.Equal(50, _positionTracker.LastTag.PreparePosition);
			Assert.Equal(100, _positionTracker.LastTag.CommitPosition);
		}
	}
}
