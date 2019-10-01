using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.event_by_type_index_position_tagger {
	public class when_updating_event_by_type_index_position_tracker_from_a_tag {
		private EventByTypeIndexPositionTagger _tagger;
		private CheckpointTag _tag;
		private PositionTracker _positionTracker;

		public when_updating_event_by_type_index_position_tracker_from_a_tag() {
			// given
			var tagger = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var tracker = new PositionTracker(tagger);

			var newTag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(10, 5),
				new Dictionary<string, long> {{"type1", 1}, {"type2", 2}});

			tracker.UpdateByCheckpointTagInitial(newTag);
			_tag = tracker.LastTag;
			_tagger = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			_positionTracker = new PositionTracker(_tagger);
			// when 

			_positionTracker.UpdateByCheckpointTagInitial(_tag);
		}

		[Fact]
		public void stream_position_is_updated() {
			Assert.Equal(1, _positionTracker.LastTag.Streams["type1"]);
			Assert.Equal(2, _positionTracker.LastTag.Streams["type2"]);
		}

		[Fact]
		public void tf_stream_position_is_updated() {
			Assert.Equal(new TFPos(10, 5), _positionTracker.LastTag.Position);
		}
	}
}
