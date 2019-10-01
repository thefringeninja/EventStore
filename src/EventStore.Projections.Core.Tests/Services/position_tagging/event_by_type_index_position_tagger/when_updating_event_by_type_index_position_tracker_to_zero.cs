using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.event_by_type_index_position_tagger {
	public class when_updating_event_by_type_index_position_tracker_to_zero {
		private EventByTypeIndexPositionTagger _tagger;
		private PositionTracker _positionTracker;

		public when_updating_event_by_type_index_position_tracker_to_zero() {
			_tagger = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			_positionTracker = new PositionTracker(_tagger);
			// when 

			_positionTracker.UpdateByCheckpointTagInitial(_tagger.MakeZeroCheckpointTag());
		}

		[Fact]
		public void streams_are_set_up() {
			Assert.Contains("type1", _positionTracker.LastTag.Streams.Keys);
			Assert.Contains("type2", _positionTracker.LastTag.Streams.Keys);
		}

		[Fact]
		public void stream_position_is_minus_one() {
			Assert.Equal(ExpectedVersion.NoStream, _positionTracker.LastTag.Streams["type1"]);
			Assert.Equal(ExpectedVersion.NoStream, _positionTracker.LastTag.Streams["type2"]);
		}

		[Fact]
		public void tf_position_is_zero() {
			Assert.Equal(new TFPos(0, -1), _positionTracker.LastTag.Position);
		}
	}
}
