using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.multistream_position_tagger {
	public class when_updating_multistream_postion_tracker_to_zero {
		private MultiStreamPositionTagger _tagger;
		private PositionTracker _positionTracker;

		public when_updating_multistream_postion_tracker_to_zero() {
			_tagger = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			_positionTracker = new PositionTracker(_tagger);
			// when 

			_positionTracker.UpdateByCheckpointTagInitial(_tagger.MakeZeroCheckpointTag());
		}

		[Fact]
		public void streams_are_set_up() {
			Assert.Contains("stream1", _positionTracker.LastTag.Streams.Keys);
			Assert.Contains("stream2", _positionTracker.LastTag.Streams.Keys);
		}

		[Fact]
		public void stream_position_is_minus_one() {
			Assert.Equal(ExpectedVersion.NoStream, _positionTracker.LastTag.Streams["stream1"]);
			Assert.Equal(ExpectedVersion.NoStream, _positionTracker.LastTag.Streams["stream2"]);
		}
	}
}
