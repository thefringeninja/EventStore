using System.Linq;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class when_loading_a_new_projection : TestFixtureWithCoreProjectionLoaded {
		protected override void Given() {
			NoStream("$projections-projection-result");
			NoStream("$projections-projection-order");
			AllWritesToSucceed("$projections-projection-order");
			NoStream("$projections-projection-checkpoint");
		}

		protected override void When() {
		}

		[Fact]
		public void should_not_subscribe() {
			Assert.Equal(0, _subscribeProjectionHandler.HandledMessages.Count);
		}

		[Fact]
		public void should_not_initialize_projection_state_handler() {
			Assert.Equal(0, _stateHandler._initializeCalled);
		}

		[Fact]
		public void should_not_publish_started_message() {
			Assert.Equal(0, Consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Started>().Count());
		}
	}
}
