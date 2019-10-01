using System;
using System.Collections.Generic;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.multistream_position_tagger {
	public class when_updating_postion_multistream_position_tracker {
		private MultiStreamPositionTagger _tagger;
		private PositionTracker _positionTracker;

		public when_updating_postion_multistream_position_tracker() {
			// given
			_tagger = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			_positionTracker = new PositionTracker(_tagger);
			var newTag =
				CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 1}, {"stream2", 2}});
			var newTag2 =
				CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 1}, {"stream2", 3}});
			_positionTracker.UpdateByCheckpointTagInitial(newTag);
			_positionTracker.UpdateByCheckpointTagForward(newTag2);
		}

		[Fact]
		public void stream_position_is_updated() {
			Assert.Equal(1, _positionTracker.LastTag.Streams["stream1"]);
			Assert.Equal(3, _positionTracker.LastTag.Streams["stream2"]);
		}


		[Fact]
		public void cannot_update_to_the_same_postion() {
			Assert.Throws<InvalidOperationException>(() => {
				var newTag =
					CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 1}, {"stream2", 3}});
				_positionTracker.UpdateByCheckpointTagForward(newTag);
			});
		}

		[Fact]
		public void it_cannot_be_updated_with_other_stream() {
			Assert.Throws<InvalidOperationException>(() => {
				// even not initialized (UpdateToZero can be removed)
				var newTag =
					CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 3}, {"stream3", 2}});
				_positionTracker.UpdateByCheckpointTagForward(newTag);
			});
		}

		//TODO: write tests on updating with incompatible snapshot loaded
	}
}
