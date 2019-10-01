using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.stream_position_tagger {
	public class when_updating_stream_postion_tracker_to_zero {
		private StreamPositionTagger _tagger;
		private PositionTracker _positionTracker;

		public when_updating_stream_postion_tracker_to_zero() {
			_tagger = new StreamPositionTagger(0, "stream1");
			_positionTracker = new PositionTracker(_tagger);
			// when 

			_positionTracker.UpdateByCheckpointTagInitial(_tagger.MakeZeroCheckpointTag());
		}

		[Fact]
		public void streams_are_set_up() {
			Assert.Contains("stream1", _positionTracker.LastTag.Streams.Keys);
		}

		[Fact]
		public void stream_position_is_minus_one() {
			Assert.Equal(-1, _positionTracker.LastTag.Streams["stream1"]);
		}
	}
}
