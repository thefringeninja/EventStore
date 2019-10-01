using System;
using System.Threading;
using EventStore.Core.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Helpers.IODispatcherTests.ReadEventsTests {
	public class async_read_stream_events_backward_with_timeout_on_read : with_read_io_dispatcher {
		private bool _didTimeout;
		private bool _didReceiveRead;


		public async_read_stream_events_backward_with_timeout_on_read() {
			var mre = new ManualResetEvent(false);
			var step = _ioDispatcher.BeginReadBackward(
				_cancellationScope, _eventStreamId, _fromEventNumber, _maxCount, true, _principal,
				res => {
					_didReceiveRead = true;
					mre.Set();
				},
				() => {
					_didTimeout = true;
					mre.Set();
				}
			);
			step.Run();
			Assert.NotNull(_timeoutMessage);

			_timeoutMessage.Reply();
			mre.WaitOne(TimeSpan.FromSeconds(10));
		}

		[Fact]
		public void should_call_timeout_handler() {
			Assert.True(_didTimeout);
		}

		[Fact]
		public void should_ignore_read_complete() {
			Assert.False(_didReceiveRead, "Should not have received read completed before replying on message");
			_readBackward.Envelope.ReplyWith(CreateReadStreamEventsBackwardCompleted(_readBackward));
			Assert.False(_didReceiveRead);
		}
	}

	public class read_stream_events_backward_with_timeout_on_read : with_read_io_dispatcher {
		private bool _didTimeout;
		private bool _didReceiveRead;

		public read_stream_events_backward_with_timeout_on_read() {
			var mre = new ManualResetEvent(false);
			_ioDispatcher.ReadBackward(
				_eventStreamId, _fromEventNumber, _maxCount, true, _principal,
				res => {
					_didReceiveRead = true;
					mre.Set();
				},
				() => {
					_didTimeout = true;
					mre.Set();
				},
				Guid.NewGuid()
			);
			Assert.NotNull(_timeoutMessage);

			_timeoutMessage.Reply();
			mre.WaitOne(TimeSpan.FromSeconds(10));
		}

		[Fact]
		public void should_call_timeout_handler() {
			Assert.True(_didTimeout);
		}

		[Fact]
		public void should_ignore_read_complete() {
			Assert.False(_didReceiveRead, "Should not have received read completed before replying on message");
			_readBackward.Envelope.ReplyWith(CreateReadStreamEventsBackwardCompleted(_readBackward));
			Assert.False(_didReceiveRead);
		}
	}
}
