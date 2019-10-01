using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexVAny {
	public class create_index_map_from_non_existing_file {
		private IndexMap _map;

		public create_index_map_from_non_existing_file() {
			_map = IndexMapTestFactory.FromFile("thisfiledoesnotexist");
		}

		[Fact]
		public void the_map_is_empty() {
			Assert.Equal(0, _map.InOrder().Count());
		}

		[Fact]
		public void no_file_names_are_used() {
			Assert.Equal(0, _map.GetAllFilenames().Count());
		}

		[Fact]
		public void prepare_checkpoint_is_equal_to_minus_one() {
			Assert.Equal(-1, _map.PrepareCheckpoint);
		}

		[Fact]
		public void commit_checkpoint_is_equal_to_minus_one() {
			Assert.Equal(-1, _map.CommitCheckpoint);
		}
	}
}
