using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.stream_position_tagger {
	public class when_updating_stream_postion_tracker_from_a_tag {
		private StreamPositionTagger _tagger;
		private CheckpointTag _tag;
		private PositionTracker _positionTracker;

		public when_updating_stream_postion_tracker_from_a_tag() {
			// given
			var tagger = new StreamPositionTagger(0, "stream1");
			var tracker = new PositionTracker(tagger);

			var newTag = CheckpointTag.FromStreamPosition(0, "stream1", 1);
			tracker.UpdateByCheckpointTagInitial(newTag);
			_tag = tracker.LastTag;
			_tagger = new StreamPositionTagger(0, "stream1");
			_positionTracker = new PositionTracker(_tagger);
			// when 

			_positionTracker.UpdateByCheckpointTagInitial(_tag);
		}

		[Fact]
		public void stream_position_is_updated() {
			Assert.Equal(1, _positionTracker.LastTag.Streams["stream1"]);
		}
	}
}
