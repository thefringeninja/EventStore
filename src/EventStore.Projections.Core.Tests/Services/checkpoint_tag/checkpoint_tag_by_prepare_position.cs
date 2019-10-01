#pragma warning disable 1718

using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	public class checkpoint_tag_by_prepare_position {
		private readonly CheckpointTag _aa = CheckpointTag.FromPreparePosition(1, 9);
		private readonly CheckpointTag _b1 = CheckpointTag.FromPreparePosition(1, 15);
		private readonly CheckpointTag _b2 = CheckpointTag.FromPreparePosition(1, 15);
		private readonly CheckpointTag _cc = CheckpointTag.FromPreparePosition(1, 29);
		private readonly CheckpointTag _d1 = CheckpointTag.FromPreparePosition(1, 35);
		private readonly CheckpointTag _d2 = CheckpointTag.FromPreparePosition(1, 35);

		[Fact]
		public void equal_equals() {
			Assert.True(_aa.Equals(_aa));
		}

		[Fact]
		public void equal_operator() {
			Assert.True(_b1 == _b1);
			Assert.True(_b1 == _b2);
		}

		[Fact]
		public void less_operator() {
			Assert.True(_aa < _b1);
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
			Assert.False(_d2 > _d1);
			Assert.False(_d2 > _d2);
		}

		[Fact]
		public void greater_or_equal_operator() {
			Assert.True(_d1 >= _cc);
			Assert.True(_d2 >= _d1);
			Assert.True(_b2 >= _b2);
		}
	}
}
