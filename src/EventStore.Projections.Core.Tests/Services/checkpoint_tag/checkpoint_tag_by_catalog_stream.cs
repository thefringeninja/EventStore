using EventStore.Projections.Core.Services.Processing;
using Xunit;

#pragma warning disable 1718 // allow a == a comparison

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	public class checkpoint_tag_by_stream_position {
		private readonly CheckpointTag _a = CheckpointTag.FromStreamPosition(1, "stream", 9);
		private readonly CheckpointTag _b = CheckpointTag.FromStreamPosition(1, "stream", 15);
		private readonly CheckpointTag _c = CheckpointTag.FromStreamPosition(1, "stream", 29);

		[Fact]
		public void equal_equals() {
			Assert.True(_a.Equals(_a));
		}

		[Fact]
		public void equal_operator() {
			Assert.True(_b == _b);
		}

		[Fact]
		public void less_operator() {
			Assert.True(_a < _b);
		}

		[Fact]
		public void less_or_equal_operator() {
			Assert.True(_a <= _b);
			Assert.True(_c <= _c);
		}

		[Fact]
		public void greater_operator() {
			Assert.True(_b > _a);
		}

		[Fact]
		public void greater_or_equal_operator() {
			Assert.True(_b >= _a);
			Assert.True(_c >= _c);
		}
	}
#pragma warning restore 1718
}
