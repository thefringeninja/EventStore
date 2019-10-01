using EventStore.ClientAPI.Common;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_filter {
	public class include_everything_handling_deleted_notifications_event_filter : TestFixtureWithEventFilter {
		protected override void Given() {
			_builder.FromAll();
			_builder.AllEvents();
			_builder.SetHandlesStreamDeletedNotifications();
			_builder.SetByStream();
		}

		[Fact]
		public void can_be_built() {
			Assert.NotNull(_ef);
		}

		[Fact]
		public void does_not_pass_categorized_event() {
			Assert.False(_ef.Passes(true, "$ce-stream", "event"));
		}

		[Fact]
		public void passes_uncategorized_event() {
			Assert.True(_ef.Passes(false, "stream", "event"));
		}

		[Fact]
		public void does_not_pass_stream_deleted_event() {
			Assert.False(_ef.Passes(false, "stream", SystemEventTypes.StreamMetadata, isStreamDeletedEvent: true));
		}
	}
}
