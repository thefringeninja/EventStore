using EventStore.Projections.Core.Services.Processing;
using Xunit;

#pragma warning disable 1718 // allow a == a comparison

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	public class checkpoint_tag_by_catalog_stream {
		private readonly CheckpointTag _a = CheckpointTag.FromByStreamPosition(0, "catalog", 1, "data", 10, 12345);
		private readonly CheckpointTag _b = CheckpointTag.FromByStreamPosition(0, "catalog", 1, "data", 20, 12345);
		private readonly CheckpointTag _c = CheckpointTag.FromByStreamPosition(0, "catalog", 2, "data2", 20, 12345);

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
