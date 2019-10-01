using System;
using EventStore.Projections.Core.Messages;
using Xunit;
using System.Linq;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class when_starting_a_new_projection : TestFixtureWithCoreProjectionStarted {
		protected override void Given() {
			NoStream("$projections-projection-result");
			NoStream("$projections-projection-order");
			AllWritesToSucceed("$projections-projection-order");
			NoStream("$projections-projection-checkpoint");
		}

		protected override void When() {
		}

		[Fact]
		public void should_subscribe_from_beginning() {
			Assert.Equal(1, _subscribeProjectionHandler.HandledMessages.Count);
			Assert.Equal(0, _subscribeProjectionHandler.HandledMessages[0].FromPosition.Position.CommitPosition);
			Assert.Equal(-1, _subscribeProjectionHandler.HandledMessages[0].FromPosition.Position.PreparePosition);
		}

		[Fact]
		public void should_publish_started_message() {
			Assert.Equal(1, Consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Started>().Count());
			var startedMessage = Consumer.HandledMessages.OfType<CoreProjectionStatusMessage.Started>().Single();
			Assert.Equal(_projectionCorrelationId, startedMessage.ProjectionId);
		}
	}
}
