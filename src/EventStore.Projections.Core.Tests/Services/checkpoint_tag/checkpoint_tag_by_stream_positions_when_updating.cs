using System.Collections.Generic;
using System.Linq;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	public class checkpoint_tag_by_stream_positions_when_updating {
		private readonly CheckpointTag _a1b1 = CheckpointTag.FromStreamPositions(
			1, new Dictionary<string, long> {{"a", 1}, {"b", 1}});

		[Fact]
		public void updated_position_is_correct() {
			var updated = _a1b1.UpdateStreamPosition("a", 2);
			Assert.Equal(2, updated.Streams["a"]);
		}

		[Fact]
		public void other_stream_position_is_correct() {
			var updated = _a1b1.UpdateStreamPosition("a", 2);
			Assert.Equal(1, updated.Streams["b"]);
		}

		[Fact]
		public void streams_are_correct() {
			var updated = _a1b1.UpdateStreamPosition("a", 2);
			Assert.Equal(2, updated.Streams.Count);
			Assert.True(updated.Streams.Any(v => v.Key == "a"));
			Assert.True(updated.Streams.Any(v => v.Key == "b"));
		}
	}
}
