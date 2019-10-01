using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_filter {
	public class specific_event_event_filter : TestFixtureWithEventFilter {
		protected override void Given() {
			_builder.FromAll();
			_builder.IncludeEvent("event");
		}

		[Fact]
		public void can_be_built() {
			Assert.NotNull(_ef);
		}

		[Fact]
		public void does_not_pass_categorized_event_with_correct_event_name() {
			Assert.False(_ef.Passes(true, "stream", "event"));
		}

		[Fact]
		public void does_not_pass_categorized_event_with_incorrect_event_name() {
			Assert.False(_ef.Passes(true, "stream", "incorrect_event"));
		}

		[Fact]
		public void passes_uncategorized_event_with_correct_event_name() {
			Assert.True(_ef.Passes(false, "stream", "event"));
		}

		[Fact]
		public void does_not_pass_uncategorized_event_with_incorrect_event_name() {
			Assert.False(_ef.Passes(true, "stream", "incorrect_event"));
		}
	}
}
