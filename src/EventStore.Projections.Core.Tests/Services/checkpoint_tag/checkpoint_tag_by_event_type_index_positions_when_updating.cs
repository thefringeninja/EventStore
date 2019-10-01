using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	public class checkpoint_tag_by_event_type_index_positions_when_updating {
		private readonly CheckpointTag _a1b1 = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(100, 50), new Dictionary<string, long> {{"a", 1}, {"b", 1}});

		[Fact]
		public void updated_tf_only_position_is_correct() {
			var updated = _a1b1.UpdateEventTypeIndexPosition(new TFPos(200, 50));
			Assert.Equal(1, updated.Streams["a"]);
			Assert.Equal(new TFPos(200, 50), updated.Position);
		}

		[Fact]
		public void updated_tf_position_is_correct() {
			var updated = _a1b1.UpdateEventTypeIndexPosition(new TFPos(200, 50), "a", 2);
			Assert.Equal(2, updated.Streams["a"]);
			Assert.Equal(new TFPos(200, 50), updated.Position);
		}

		[Fact]
		public void other_stream_position_is_correct() {
			var updated = _a1b1.UpdateEventTypeIndexPosition(new TFPos(200, 50), "a", 2);
			Assert.Equal(1, updated.Streams["b"]);
		}

		[Fact]
		public void streams_are_correct() {
			var updated = _a1b1.UpdateEventTypeIndexPosition(new TFPos(200, 50), "a", 2);
			Assert.Equal(2, updated.Streams.Count);
			Assert.Contains(updated.Streams, v => v.Key == "a");
			Assert.Contains(updated.Streams, v => v.Key == "b");
		}
	}
}
