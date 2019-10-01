using System;
using System.Threading;
using EventStore.Core.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Helpers.IODispatcherTests.ReadEventsTests {
	public class async_read_stream_events_backward_with_cancelled_read : with_read_io_dispatcher {
		private bool _hasTimedOut;
		private bool _hasRead;
		private bool _eventSet;

		public async_read_stream_events_backward_with_cancelled_read() {
			var mre = new ManualResetEvent(false);
			var step = _ioDispatcher.BeginReadBackward(
				_cancellationScope, _eventStreamId, _fromEventNumber, _maxCount, true, _principal,
				res => {
					_hasRead = true;
					mre.Set();
				},
				() => {
					_hasTimedOut = true;
					mre.Set();
				}
			);

			step.Run();
			_cancellationScope.Cancel();
			_eventSet = mre.WaitOne(TimeSpan.FromSeconds(5));
		}

		[Fact]
		public void should_ignore_read() {
			Assert.False(_eventSet);
			Assert.False(_hasRead, "Should not have completed read before replying on read message");
			_readBackward.Envelope.ReplyWith(CreateReadStreamEventsBackwardCompleted(_readBackward));
			Assert.False(_hasRead);
		}

		[Fact]
		public void should_ignore_timeout_message() {
			Assert.False(_hasTimedOut, "Should not have timed out before replying on timeout message");
			_timeoutMessage.Reply();
			Assert.False(_hasTimedOut);
		}
	}
}
