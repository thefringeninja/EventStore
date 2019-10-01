using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_filter {
	public class specific_events_event_filter : TestFixtureWithEventFilter {
		protected override void Given() {
			_builder.FromAll();
			_builder.IncludeEvent("eventOne");
			_builder.IncludeEvent("eventTwo");
		}

		[Fact]
		public void can_be_built() {
			Assert.NotNull(_ef);
		}

		[Fact]
		public void should_allow_non_linked_events() {
			Assert.True(_ef.Passes(false, "stream", "eventOne"));
		}

		[Fact]
		public void should_allow_events_from_event_type_stream() {
			Assert.True(_ef.Passes(true, "$et-eventOne", "eventOne"));
		}

		[Fact]
		public void should_not_allow_events_from_event_type_stream_that_is_not_included() {
			Assert.False(_ef.Passes(true, "$et-eventThree", "eventThree"));
		}

		[Fact]
		public void should_not_allow_events_from_system_streams() {
			Assert.False(_ef.Passes(false, "$ct-test", "eventOne"));
		}

		[Fact]
		public void should_not_allow_linked_events_from_system_streams() {
			Assert.False(_ef.Passes(true, "$ct-test", "eventOne"));
		}
	}
}
