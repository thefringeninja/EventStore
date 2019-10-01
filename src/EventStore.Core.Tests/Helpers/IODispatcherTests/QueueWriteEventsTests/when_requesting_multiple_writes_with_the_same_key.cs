using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.UserManagement;
using Xunit;
using System;
using System.Linq;

namespace EventStore.Core.Tests.Helpers.IODispatcherTests.QueueWriteEventsTests {
	public class when_requesting_multiple_writes_with_the_same_key : TestFixtureWithExistingEvents {
		protected override void Given() {
			AllWritesQueueUp();

			var key = Guid.NewGuid();
			_ioDispatcher.QueueWriteEvents(key, $"stream-{Guid.NewGuid()}", ExpectedVersion.Any,
				new Event[] {new Event(Guid.NewGuid(), "event-type", false, string.Empty, string.Empty)},
				SystemAccount.Principal, (msg) => { });
			_ioDispatcher.QueueWriteEvents(key, $"stream-{Guid.NewGuid()}", ExpectedVersion.Any,
				new Event[] {new Event(Guid.NewGuid(), "event-type", false, string.Empty, string.Empty)},
				SystemAccount.Principal, (msg) => { });
			_ioDispatcher.QueueWriteEvents(key, $"stream-{Guid.NewGuid()}", ExpectedVersion.Any,
				new Event[] {new Event(Guid.NewGuid(), "event-type", false, string.Empty, string.Empty)},
				SystemAccount.Principal, (msg) => { });
		}

		[Fact]
		public void should_only_have_a_single_write_in_flight() {
			Assert.Equal(1, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
		}

		[Fact]
		public void should_continue_to_only_have_a_single_write_in_flight_as_writes_complete() {
			var writeRequests = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>();

			//first write
			Consumer.HandledMessages.Clear();
			OneWriteCompletes();
			Assert.Equal(1, writeRequests.Count());

			//second write
			Consumer.HandledMessages.Clear();
			OneWriteCompletes();
			Assert.Equal(1, writeRequests.Count());

			//third write completes, no more writes left in the queue
			Consumer.HandledMessages.Clear();
			OneWriteCompletes();
			Assert.Equal(0, writeRequests.Count());
		}
	}
}
