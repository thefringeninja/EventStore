using EventStore.Core.Data;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services {
#pragma warning disable 1718 // allow a == a comparison
	public class event_position {
		private readonly TFPos _aa = new TFPos(10, 9);
		private readonly TFPos _b1 = new TFPos(20, 15);
		private readonly TFPos _b2 = new TFPos(20, 17);
		private readonly TFPos _cc = new TFPos(30, 29);
		private readonly TFPos _d1 = new TFPos(40, 35);
		private readonly TFPos _d2 = new TFPos(40, 36);

		[Fact]
		public void equal_equals() {
			Assert.True(_aa.Equals(_aa));
		}

		[Fact]
		public void equal_operator() {
			Assert.True(_b1 == _b1);
		}

		[Fact]
		public void less_operator() {
			Assert.True(_aa < _b1);
			Assert.True(_b1 < _b2);
		}

		[Fact]
		public void less_or_equal_operator() {
			Assert.True(_aa <= _b1);
			Assert.True(_b1 <= _b2);
			Assert.True(_b2 <= _b2);
		}

		[Fact]
		public void greater_operator() {
			Assert.True(_d1 > _cc);
			Assert.True(_d2 > _d1);
		}

		[Fact]
		public void greater_or_equal_operator() {
			Assert.True(_d1 >= _cc);
			Assert.True(_d2 >= _d1);
			Assert.True(_b2 >= _b2);
		}
	}
#pragma warning restore 1718
}
