using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index {
	public class ReverseComparerTests {
		[Fact]
		public void larger_values_return_as_lower() {
			Assert.Equal(-1, new ReverseComparer<int>().Compare(5, 3));
		}

		[Fact]
		public void smaller_values_return_as_higher() {
			Assert.Equal(1, new ReverseComparer<int>().Compare(3, 5));
		}

		[Fact]
		public void same_values_are_equal() {
			Assert.Equal(0, new ReverseComparer<int>().Compare(5, 5));
		}
	}
}
