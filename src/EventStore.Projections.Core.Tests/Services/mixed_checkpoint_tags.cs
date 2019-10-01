using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services {
	public class mixed_checkpoint_tags {
		private readonly CheckpointTag _a = CheckpointTag.FromStreamPosition(0, "stream1", 9);
		private readonly CheckpointTag _b = CheckpointTag.FromStreamPosition(0, "stream2", 15);
		private readonly CheckpointTag _c = CheckpointTag.FromPosition(0, 50, 29);

		[Fact]
		public void are_not_equal() {
			Assert.NotEqual(_a, _b);
			Assert.NotEqual(_a, _c);
			Assert.NotEqual(_b, _c);

			Assert.True(_a != _b);
			Assert.True(_a != _c);
			Assert.True(_b != _c);
		}

		[Fact]
		public void cannot_be_compared() {
			Assert.True(throws(() => _a > _b));
			Assert.True(throws(() => _a >= _b));
			Assert.True(throws(() => _a > _c));
			Assert.True(throws(() => _a >= _c));
			Assert.True(throws(() => _b > _c));
			Assert.True(throws(() => _b >= _c));
			Assert.True(throws(() => _a < _b));
			Assert.True(throws(() => _a <= _b));
			Assert.True(throws(() => _a < _c));
			Assert.True(throws(() => _a <= _c));
			Assert.True(throws(() => _b < _c));
			Assert.True(throws(() => _b <= _c));
		}

		private bool throws(Func<bool> func) {
			try {
				func();
				return false;
			} catch (Exception) {
				return true;
			}
		}
	}
}
