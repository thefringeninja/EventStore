using System.Collections.Generic;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.multistream_position_tagger {
	public class when_updating_multistream_postion_tracker_from_a_tag {
		private MultiStreamPositionTagger _tagger;
		private CheckpointTag _tag;
		private PositionTracker _positionTracker;

		public when_updating_multistream_postion_tracker_from_a_tag() {
			// given
			var tagger = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			var tracker = new PositionTracker(tagger);

			var newTag =
				CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 1}, {"stream2", 2}});
			tracker.UpdateByCheckpointTagInitial(newTag);
			_tag = tracker.LastTag;
			_tagger = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			_positionTracker = new PositionTracker(_tagger);
			// when 

			_positionTracker.UpdateByCheckpointTagInitial(_tag);
		}

		[Fact]
		public void stream_position_is_updated() {
			Assert.Equal(1, _positionTracker.LastTag.Streams["stream1"]);
			Assert.Equal(2, _positionTracker.LastTag.Streams["stream2"]);
		}
	}
}
