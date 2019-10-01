using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.event_by_type_index_position_tagger {
	public class when_updating_postion_event_by_type_index_position_tracker {
		private EventByTypeIndexPositionTagger _tagger;
		private PositionTracker _positionTracker;

		public when_updating_postion_event_by_type_index_position_tracker() {
			// given
			_tagger = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			_positionTracker = new PositionTracker(_tagger);
			var newTag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(10, 5),
				new Dictionary<string, long> {{"type1", 1}, {"type2", 2}});
			var newTag2 = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(20, 15),
				new Dictionary<string, long> {{"type1", 1}, {"type2", 3}});
			_positionTracker.UpdateByCheckpointTagInitial(newTag);
			_positionTracker.UpdateByCheckpointTagForward(newTag2);
		}

		[Fact]
		public void stream_position_is_updated() {
			Assert.Equal(1, _positionTracker.LastTag.Streams["type1"]);
			Assert.Equal(3, _positionTracker.LastTag.Streams["type2"]);
		}

		[Fact]
		public void tf_position_is_updated() {
			Assert.Equal(new TFPos(20, 15), _positionTracker.LastTag.Position);
		}

		[Fact]
		public void cannot_update_to_the_same_position() {
			var newTag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(20, 15),
				new Dictionary<string, long> {{"type1", 1}, {"type2", 3}});
			Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagForward(newTag); });
		}

		[Fact]
		public void can_update_to_the_same_index_position_but_tf() {
			var newTag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(30, 25),
				new Dictionary<string, long> {{"type1", 1}, {"type2", 3}});
			_positionTracker.UpdateByCheckpointTagForward(newTag);
		}

		[Fact]
		public void it_cannot_be_updated_with_other_stream() {
			// even not initialized (UpdateToZero can be removed)
			var newTag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(30, 25),
				new Dictionary<string, long> {{"type1", 1}, {"type3", 3}});
			Assert.Throws<InvalidOperationException>(() => { _positionTracker.UpdateByCheckpointTagForward(newTag); });
		}

		//TODO: write tests on updating with incompatible snapshot loaded
	}
}
