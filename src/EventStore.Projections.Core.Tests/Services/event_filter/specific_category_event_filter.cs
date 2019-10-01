using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_filter {
	public class specific_category_event_filter : TestFixtureWithEventFilter {
		protected override void Given() {
			_builder.FromCategory("category");
			_builder.AllEvents();
		}

		[Fact]
		public void can_be_built() {
			Assert.NotNull(_ef);
		}

		[Fact]
		public void passes_event_with_correct_category() {
			Assert.True(_ef.Passes(true, "$ce-category", "event"));
		}

		[Fact]
		public void does_not_pass_event_with_incorrect_category() {
			Assert.False(_ef.Passes(true, "$ce-another", "event"));
		}

		[Fact]
		public void does_not_pass_uncategorized_event() {
			Assert.False(_ef.Passes(false, "stream", "event"));
		}
	}
}
