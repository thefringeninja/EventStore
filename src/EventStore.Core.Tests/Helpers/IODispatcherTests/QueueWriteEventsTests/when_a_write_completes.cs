using EventStore.Core.Data;
using EventStore.Core.Services.UserManagement;
using Xunit;
using System;

namespace EventStore.Core.Tests.Helpers.IODispatcherTests.QueueWriteEventsTests {
	public class when_a_write_completes : TestFixtureWithExistingEvents {
		private bool _completed = false;

		protected override void Given() {
			AllWritesQueueUp();

			_ioDispatcher.QueueWriteEvents(Guid.NewGuid(), $"stream-{Guid.NewGuid()}", ExpectedVersion.Any,
				new Event[] {new Event(Guid.NewGuid(), "event-type", false, string.Empty, string.Empty)},
				SystemAccount.Principal, (msg) => { _completed = true; });
			OneWriteCompletes();
		}

		[Fact]
		public void should_invoke_callback_when_write_completes() {
			Assert.True(_completed);
		}

		protected override ManualQueue GiveInputQueue() => null;
	}
}
