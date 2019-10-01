using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_filter {
	public class specific_stream_event_filter : TestFixtureWithEventFilter {
		protected override void Given() {
			_builder.FromStream("/test");
			_builder.AllEvents();
		}

		[Fact]
		public void can_be_built() {
			Assert.NotNull(_ef);
		}

		[Fact]
		public void passes_categorized_event_with_correct_stream_id() {
			//NOTE: this is possible if you read from $ce-account stream
			// this is not the same as reading an account category as you can see at 
			// least StreamCreate even there
			Assert.True(_ef.Passes(true, "/test", "event"));
		}

		[Fact]
		public void does_not_pass_categorized_event_with_incorrect_stream_id() {
			Assert.False(_ef.Passes(true, "incorrect_stream", "event"));
		}

		[Fact]
		public void passes_uncategorized_event_with_correct_stream_id() {
			Assert.True(_ef.Passes(false, "/test", "event"));
		}

		[Fact]
		public void does_not_pass_uncategorized_event_with_incorrect_stream_id() {
			Assert.False(_ef.Passes(true, "incorrect_stream", "event"));
		}
	}
}
