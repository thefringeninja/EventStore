using System;
using System.Collections.Generic;
using EventStore.Core.Tests.Helpers;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

#pragma warning disable 1718 // allow a == a comparison

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	public class checkpoint_tag_by_stream_positions {
		private readonly CheckpointTag _a1 = CheckpointTag.FromStreamPositions(
			1, new Dictionary<string, long> {{"a", 1}});

		private readonly CheckpointTag _b1 = CheckpointTag.FromStreamPositions(
			1, new Dictionary<string, long> {{"b", 1}});

		private readonly CheckpointTag _a1b1 = CheckpointTag.FromStreamPositions(
			1, new Dictionary<string, long> {{"a", 1}, {"b", 1}});

		private readonly CheckpointTag _a2b1 = CheckpointTag.FromStreamPositions(
			1, new Dictionary<string, long> {{"a", 2}, {"b", 1}});

		private readonly CheckpointTag _a1b2 = CheckpointTag.FromStreamPositions(
			1, new Dictionary<string, long> {{"a", 1}, {"b", 2}});

		private readonly CheckpointTag _a2b2 = CheckpointTag.FromStreamPositions(
			1, new Dictionary<string, long> {{"a", 2}, {"b", 2}});

		[Fact]
		public void equal_equals() {
			Assert.True(_a1.Equals(_a1));
		}

		[Fact]
		public void equal_operator() {
			Assert.True(_a2b1 == _a2b1);
		}

		[Fact]
		public void less_operator() {
			Assert.True(_a1 < _a1b1);
			Assert.True(_a1 < _a1b2);
			Assert.True(_a1 < _a2b2);
			Assert.False(_a1b2 < _a1b2);
			Assert.False(_a1b2 < _a1b1);
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(_a1b2 < _a2b1));
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(_a1 < _b1));
		}

		[Fact]
		public void less_or_equal_operator() {
			Assert.True(_a1 <= _a1b1);
			Assert.True(_a1 <= _a1b2);
			Assert.True(_a1 <= _a2b2);
			Assert.True(_a1b2 <= _a1b2);
			Assert.False(_a1b2 <= _a1b1);
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(_a1b2 <= _a2b1));
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(_a1 <= _b1));
		}

		[Fact]
		public void greater_operator() {
			Assert.False(_a1 > _a1b1);
			Assert.False(_a1 > _a1b2);
			Assert.False(_a1 > _a2b2);
			Assert.False(_a1b2 > _a1b2);
			Assert.True(_a1b2 > _a1b1);
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(_a1b2 > _a2b1));
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(_a1 > _b1));
		}

		[Fact]
		public void greater_or_equal_operator() {
			Assert.False(_a1 >= _a1b1);
			Assert.False(_a1 >= _a1b2);
			Assert.False(_a1 >= _a2b2);
			Assert.True(_a1b2 >= _a1b2);
			Assert.True(_a1b2 >= _a1b1);
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(_a1b2 >= _a2b1));
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(_a1 >= _b1));
		}
	}
#pragma warning restore 1718
}
