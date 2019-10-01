using Xunit;

namespace EventStore.Core.Tests.Services.Storage.DeletingStream {
	public class when_deleting_the_only_existing_stream_in_db_read_index_should : ReadIndexTestScenario {
		protected override void WriteTestScenario() {
			WriteSingleEvent("ES", 0, "bla1");

			WriteDelete("ES");
		}

		[Fact]
		public void indicate_that_stream_is_deleted() {
			Assert.True(ReadIndex.IsStreamDeleted("ES"));
		}

		[Fact]
		public void indicate_that_nonexisting_stream_with_same_hash_is_not_deleted() {
			Assert.False(ReadIndex.IsStreamDeleted("ZZ"));
		}

		[Fact]
		public void indicate_that_nonexisting_stream_with_different_hash_is_not_deleted() {
			Assert.False(ReadIndex.IsStreamDeleted("XXX"));
		}
	}
}
