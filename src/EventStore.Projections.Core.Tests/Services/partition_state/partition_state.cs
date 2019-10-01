using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state {
	public static class partition_state {
		public class when_creating {
			[Fact]
			public void throws_argument_null_exception_if_state_is_null() {
				Assert.Throws<ArgumentNullException>(() => {
					new PartitionState(null, "result", CheckpointTag.FromPosition(0, 100, 50));
				});
			}

			[Fact]
			public void throws_argument_null_exception_if_caused_by_is_null() {
				Assert.Throws<ArgumentNullException>(() => { new PartitionState("state", "result", null); });
			}

			[Fact]
			public void can_be_created() {
				new PartitionState("state", "result", CheckpointTag.FromPosition(0, 100, 50));
			}
		}

		public class can_be_deserialized_from_serialized_form {
			[Fact]
			public void simple_object() {
				AssertCorrect(@"");
				AssertCorrect(@"{""a"":""b""}");
				AssertCorrect(@"{""a"":""b"",""c"":1}");
				AssertCorrect(@"{""z"":null,""a"":""b"",""c"":1}");
			}

			[Fact]
			public void complex_object() {
				AssertCorrect(@"{""a"":""b"",""c"":[1,2,3]}");
				AssertCorrect(@"{""a"":""b"",""c"":{""a"":""b""}}");
				AssertCorrect(@"{""a"":""b"",""c"":[{},[],null]}");
			}

			[Fact]
			public void array() {
				AssertCorrect(@"[]");
				AssertCorrect(@"[""one"",""two""]");
				AssertCorrect(@"[{""data"":{}}]");
			}

			[Fact]
			public void null_deserialization() {
				var deserialized = PartitionState.Deserialize(null, CheckpointTag.FromPosition(0, 100, 50));
				Assert.Equal("", deserialized.State);
				Assert.Null(deserialized.Result);
			}

			private void AssertCorrect(string state, string result = null) {
				var partitionState = new PartitionState(state, result, CheckpointTag.FromPosition(0, 100, 50));
				var serialized = partitionState.Serialize();
				var deserialized = PartitionState.Deserialize(serialized, CheckpointTag.FromPosition(0, 100, 50));

				Assert.Equal(partitionState.State, deserialized.State);
				Assert.Equal(partitionState.Result, deserialized.Result);
			}
		}
	}
}
