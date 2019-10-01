using System.Threading.Tasks;
using EventStore.ClientAPI;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class read_event_of_linkto_to_deleted_event : SpecificationWithLinkToToDeletedEvents {
		private EventReadResult _read;

		protected override async Task When() {
			_read = await _conn.ReadEventAsync(LinkedStreamName, 0, true);
		}

		[Fact]
		public void the_linked_event_is_returned() {
			Assert.NotNull(_read.Event.Value.Link);
		}

		[Fact]
		public void the_deleted_event_is_not_resolved() {
			Assert.Null(_read.Event.Value.Event);
		}

		[Fact]
		public void the_status_is_success() {
			Assert.Equal(EventReadStatus.Success, _read.Status);
		}
	}

	[Trait("Category", "LongRunning")]
	public class read_allevents_backward_with_linkto_deleted_event : SpecificationWithLinkToToDeletedEvents {
		private StreamEventsSlice _read;

		protected override async Task When() {
			_read = await _conn.ReadStreamEventsBackwardAsync(LinkedStreamName, 0, 1, true, null);
		}

		[Fact]
		public void one_event_is_read() {
			Assert.Equal(1, _read.Events.Length);
		}

		[Fact]
		public void the_linked_event_is_not_resolved() {
			Assert.Null(_read.Events[0].Event);
		}

		[Fact]
		public void the_link_event_is_included() {
			Assert.NotNull(_read.Events[0].OriginalEvent);
		}

		[Fact]
		public void the_event_is_not_resolved() {
			Assert.False(_read.Events[0].IsResolved);
		}
	}
}
