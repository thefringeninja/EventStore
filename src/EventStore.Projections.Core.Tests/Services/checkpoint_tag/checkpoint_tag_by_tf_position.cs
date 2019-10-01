using EventStore.Projections.Core.Services.Processing;
using Xunit;

#pragma warning disable 1718 // allow a == a comparison

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	public class checkpoint_tag_by_tf_position {
		private readonly CheckpointTag _aa = CheckpointTag.FromPosition(1, 10, 9);
		private readonly CheckpointTag _b1 = CheckpointTag.FromPosition(1, 20, 15);
		private readonly CheckpointTag _b2 = CheckpointTag.FromPosition(1, 20, 17);
		private readonly CheckpointTag _cc = CheckpointTag.FromPosition(1, 30, 29);
		private readonly CheckpointTag _d1 = CheckpointTag.FromPosition(1, 40, 35);
		private readonly CheckpointTag _d2 = CheckpointTag.FromPosition(1, 40, 36);

		[Fact]
		public void equal_equals() {
			Assert.True(_aa.Equals(_aa));
		}

		[Fact]
		public void equal_operator() {
			Assert.True(_b1 == _b1);
		}

		[Fact]
		public void less_operator() {
			Assert.True(_aa < _b1);
			Assert.True(_b1 < _b2);
		}

		[Fact]
		public void less_or_equal_operator() {
			Assert.True(_aa <= _b1);
			Assert.True(_b1 <= _b2);
			Assert.True(_b2 <= _b2);
		}

		[Fact]
		public void greater_operator() {
			Assert.True(_d1 > _cc);
			Assert.True(_d2 > _d1);
		}

		[Fact]
		public void greater_or_equal_operator() {
			Assert.True(_d1 >= _cc);
			Assert.True(_d2 >= _d1);
			Assert.True(_b2 >= _b2);
		}
	}
#pragma warning restore 1718
}
