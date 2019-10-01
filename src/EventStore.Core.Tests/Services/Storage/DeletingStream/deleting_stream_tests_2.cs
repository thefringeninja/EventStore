using Xunit;

namespace EventStore.Core.Tests.Services.Storage.DeletingStream {
	public class
		when_deleting_streams_with_different_hashes_spanning_through_multiple_chunks_in_db_with_1_stream_with_same_hash_read_index_should :
			ReadIndexTestScenario {
		protected override void WriteTestScenario() {
			WriteSingleEvent("ES1", 0, new string('.', 3000));
			WriteSingleEvent("ES1", 1, new string('.', 3000));
			WriteSingleEvent("ES2", 0, new string('.', 3000));

			WriteSingleEvent("ES", 0, new string('.', 3000), retryOnFail: true); // chunk 2
			WriteSingleEvent("ES", 1, new string('.', 3000));
			WriteSingleEvent("ES1", 2, new string('.', 3000));

			WriteSingleEvent("ES2", 1, new string('.', 3000), retryOnFail: true); // chunk 3
			WriteSingleEvent("ES1", 3, new string('.', 3000));
			WriteSingleEvent("ES1", 4, new string('.', 3000));

			WriteSingleEvent("ES2", 2, new string('.', 3000), retryOnFail: true); // chunk 4
			WriteSingleEvent("ES", 2, new string('.', 3000));
			WriteSingleEvent("ES", 3, new string('.', 3000));

			WriteDelete("ES1");
			WriteDelete("ES");
		}

		[Fact]
		public void indicate_that_streams_are_deleted() {
			Assert.True(ReadIndex.IsStreamDeleted("ES1"));
			Assert.True(ReadIndex.IsStreamDeleted("ES"));
		}

		[Fact]
		public void indicate_that_nonexisting_streams_with_same_hashes_are_not_deleted() {
			Assert.False(ReadIndex.IsStreamDeleted("XXX"));
			Assert.False(ReadIndex.IsStreamDeleted("XX"));
		}

		[Fact]
		public void indicate_that_nonexisting_stream_with_different_hash_is_not_deleted() {
			Assert.False(ReadIndex.IsStreamDeleted("XXXX"));
		}

		[Fact]
		public void indicate_that_existing_stream_with_different_hash_is_not_deleted() {
			Assert.False(ReadIndex.IsStreamDeleted("ES2"));
		}
	}
}
