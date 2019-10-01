using System;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using EventStore.Core.Services.AwakeReaderService;
using EventStore.Core.Tests.Bus.Helpers;
using Xunit;

namespace EventStore.Core.Tests.AwakeService {
	public class when_handling_subscribe_awake {
		private Core.Services.AwakeReaderService.AwakeService _it;
		private Exception _exception;
		private IEnvelope _envelope;

		public when_handling_subscribe_awake() {
			_exception = null;
			Given();
			When();
		}

		private void Given() {
			_it = new Core.Services.AwakeReaderService.AwakeService();

			_envelope = new NoopEnvelope();
		}

		private void When() {
			try {
				_it.Handle(
					new AwakeServiceMessage.SubscribeAwake(
						_envelope, Guid.NewGuid(), "Stream", new TFPos(1000, 500), new TestMessage()));
			} catch (Exception ex) {
				_exception = ex;
			}
		}

		[Fact]
		public void it_is_handled() {
			Assert.Null(_exception);
		}
	}
}
