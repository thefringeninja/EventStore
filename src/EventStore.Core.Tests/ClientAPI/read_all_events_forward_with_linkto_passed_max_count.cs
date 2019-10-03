using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class read_all_events_forward_with_linkto_passed_max_count : IClassFixture<read_all_events_forward_with_linkto_passed_max_count.Fixture> { public class Fixture : SpecificationWithLinkToToMaxCountDeletedEvents {
		private StreamEventsSlice _read;

		protected override async Task When() {
			_read = await _conn.ReadStreamEventsForwardAsync(LinkedStreamName, 0, 1, true);
		}

		[Fact]
		public void one_event_is_read() {
			Assert.Equal(1, _read.Events.Length);
		}
	}
}
