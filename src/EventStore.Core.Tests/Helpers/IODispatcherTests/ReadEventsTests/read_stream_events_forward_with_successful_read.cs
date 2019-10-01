using System;
using System.Threading;
using EventStore.Core.Helpers;
using EventStore.Core.Messages;
using Xunit;

namespace EventStore.Core.Tests.Helpers.IODispatcherTests.ReadEventsTests {
	public class async_read_stream_events_forward_with_successful_read : with_read_io_dispatcher {
		private ClientMessage.ReadStreamEventsForwardCompleted _result;
		private bool _hasTimedOut;

		public async_read_stream_events_forward_with_successful_read() {
			var mre = new ManualResetEvent(false);
			var step = _ioDispatcher.BeginReadForward(
				_cancellationScope, _eventStreamId, _fromEventNumber, _maxCount, true, _principal,
				res => {
					_result = res;
					mre.Set();
				},
				() => {
					_hasTimedOut = true;
					mre.Set();
				}
			);

			IODispatcherAsync.Run(step);

			_readForward.Envelope.ReplyWith(CreateReadStreamEventsForwardCompleted(_readForward));
			mre.WaitOne(TimeSpan.FromSeconds(10));
		}

		[Fact]
		public void should_get_read_result() {
			Assert.NotNull(_result);
			Assert.Equal(_maxCount, _result.Events.Length);
			Assert.Equal(_eventStreamId, _result.Events[0].OriginalStreamId);
			Assert.Equal(_fromEventNumber, _result.Events[0].OriginalEventNumber);
		}

		[Fact]
		public void should_ignore_timeout_message() {
			Assert.False(_hasTimedOut, "Should not have timed out before replying on timeout message");
			_timeoutMessage.Reply();
			Assert.False(_hasTimedOut);
		}
	}

	public class read_stream_events_forward_with_successful_read : with_read_io_dispatcher {
		private ClientMessage.ReadStreamEventsForwardCompleted _result;
		private bool _hasTimedOut;

		public read_stream_events_forward_with_successful_read() {
			var mre = new ManualResetEvent(false);
			_ioDispatcher.ReadForward(
				_eventStreamId, _fromEventNumber, _maxCount, true, _principal,
				res => {
					_result = res;
					mre.Set();
				},
				() => {
					_hasTimedOut = true;
					mre.Set();
				},
				Guid.NewGuid()
			);

			_readForward.Envelope.ReplyWith(CreateReadStreamEventsForwardCompleted(_readForward));
			mre.WaitOne(TimeSpan.FromSeconds(10));
		}

		[Fact]
		public void should_get_read_result() {
			Assert.NotNull(_result);
			Assert.Equal(_maxCount, _result.Events.Length);
			Assert.Equal(_eventStreamId, _result.Events[0].OriginalStreamId);
			Assert.Equal(_fromEventNumber, _result.Events[0].OriginalEventNumber);
		}

		[Fact]
		public void should_ignore_timeout_message() {
			Assert.False(_hasTimedOut, "Should not have timed out before replying on timeout message");
			_timeoutMessage.Reply();
			Assert.False(_hasTimedOut);
		}
	}
}
