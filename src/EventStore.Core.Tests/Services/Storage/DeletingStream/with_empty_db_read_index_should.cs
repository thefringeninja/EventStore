using Xunit;

namespace EventStore.Core.Tests.Services.Storage.DeletingStream {
	public class with_empty_db_read_index_should : ReadIndexTestScenario {
		protected override void WriteTestScenario() {
		}

		[Fact]
		public void indicate_that_any_stream_is_not_deleted() {
			Assert.False(ReadIndex.IsStreamDeleted("X"));
			Assert.False(ReadIndex.IsStreamDeleted("YY"));
			Assert.False(ReadIndex.IsStreamDeleted("ZZZ"));
		}
	}
}
