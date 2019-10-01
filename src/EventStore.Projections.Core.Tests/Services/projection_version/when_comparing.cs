using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_version {
	public class when_comparing {
		[Fact]
		public void equal() {
			var v1 = new ProjectionVersion(10, 5, 6);
			var v2 = new ProjectionVersion(10, 5, 6);

			Assert.Equal(v1, v2);
		}

		[Fact]
		public void not_equal_id() {
			var v1 = new ProjectionVersion(10, 5, 6);
			var v2 = new ProjectionVersion(11, 5, 6);

			Assert.NotEqual(v1, v2);
		}

		[Fact]
		public void not_equal_epoch() {
			var v1 = new ProjectionVersion(10, 5, 6);
			var v2 = new ProjectionVersion(11, 6, 6);

			Assert.NotEqual(v1, v2);
		}

		[Fact]
		public void not_equal_version() {
			var v1 = new ProjectionVersion(10, 5, 6);
			var v2 = new ProjectionVersion(10, 5, 7);

			Assert.NotEqual(v1, v2);
		}
	}
}
