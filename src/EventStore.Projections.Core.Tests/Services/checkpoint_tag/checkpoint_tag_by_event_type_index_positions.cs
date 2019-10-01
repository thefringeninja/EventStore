using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

#pragma warning disable 1718 // allow a == a comparison

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	public class checkpoint_tag_by_event_type_index_positions {
		private readonly CheckpointTag _a1 = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(100, 50), new Dictionary<string, long> {{"a", 1}});

		private readonly CheckpointTag _a1_prime = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(100, 50), new Dictionary<string, long> {{"a", 0}});

		private readonly CheckpointTag _b1 = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(200, 150), new Dictionary<string, long> {{"b", 1}});

		private readonly CheckpointTag _a1b1 = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(300, 250), new Dictionary<string, long> {{"a", 1}, {"b", 1}});

		private readonly CheckpointTag _a2b1 = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(400, 350), new Dictionary<string, long> {{"a", 2}, {"b", 1}});

		private readonly CheckpointTag _a2b1_after = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(430, 420), new Dictionary<string, long> {{"a", 2}, {"b", 1}});

		private readonly CheckpointTag _a1b2 = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(500, 450), new Dictionary<string, long> {{"a", 1}, {"b", 2}});

		private readonly CheckpointTag _a2b2 = CheckpointTag.FromEventTypeIndexPositions(
			1, new TFPos(600, 550), new Dictionary<string, long> {{"a", 2}, {"b", 2}});

		[Fact]
		public void equal_equals() {
			Assert.True(_a1.Equals(_a1));
		}

		[Fact]
		public void equal_but_different_index_positions_still_equals() {
			Assert.True(_a1.Equals(_a1_prime));
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
			Assert.True(_a2b1 < _a2b1_after);
			Assert.False(_a1b2 < _a1b2);
			Assert.False(_a1b2 < _a1b1);
			Assert.False(_a1b2 < _a2b1);
			Assert.True(_a1 < _b1);
		}

		[Fact]
		public void less_or_equal_operator() {
			Assert.True(_a1 <= _a1b1);
			Assert.True(_a1 <= _a1b2);
			Assert.True(_a1 <= _a2b2);
			Assert.True(_a2b1 <= _a2b1_after);
			Assert.True(_a1b2 <= _a1b2);
			Assert.False(_a1b2 <= _a1b1);
			Assert.False(_a1b2 <= _a2b1);
			Assert.True(_a1 <= _b1);
		}

		[Fact]
		public void greater_operator() {
			Assert.False(_a1 > _a1b1);
			Assert.False(_a1 > _a1b2);
			Assert.False(_a1 > _a2b2);
			Assert.False(_a2b1 > _a2b1_after);
			Assert.True(_a2b1_after > _a2b1);
			Assert.False(_a1b2 > _a1b2);
			Assert.True(_a1b2 > _a1b1);
			Assert.True(_a1b2 > _a2b1);
			Assert.False(_a1 > _b1);
		}

		[Fact]
		public void greater_or_equal_operator() {
			Assert.False(_a1 >= _a1b1);
			Assert.False(_a1 >= _a1b2);
			Assert.False(_a1 >= _a2b2);
			Assert.False(_a2b1 >= _a2b1_after);
			Assert.True(_a2b1_after >= _a2b1);
			Assert.True(_a1b2 >= _a1b2);
			Assert.True(_a1b2 >= _a1b1);
			Assert.True(_a1b2 >= _a2b1);
			Assert.False(_a1 >= _b1);
		}
	}
#pragma warning restore 1718
}
