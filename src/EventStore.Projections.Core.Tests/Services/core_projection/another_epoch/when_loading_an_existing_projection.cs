using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.another_epoch {
	public class when_starting_an_existing_projection : TestFixtureWithCoreProjectionStarted {
		private string _testProjectionState = @"{""test"":1}";

		protected override void Given() {
			_version = new ProjectionVersion(1, 2, 2);
			ExistingEvent(
				"$projections-projection-result", "Result",
				@"{""v"":1, ""c"": 100, ""p"": 50}", _testProjectionState);
			ExistingEvent(
				"$projections-projection-checkpoint", ProjectionEventTypes.ProjectionCheckpoint,
				@"{""v"":1, ""c"": 100, ""p"": 50}", _testProjectionState);
			ExistingEvent(
				"$projections-projection-result", "Result",
				@"{""v"":1, ""c"": 200, ""p"": 150}", _testProjectionState);
			ExistingEvent(
				"$projections-projection-result", "Result",
				@"{""v"":1, ""c"": 300, ""p"": 250}", _testProjectionState);
		}

		protected override void When() {
		}


		[Fact]
		public void should_subscribe_from_the_beginning() {
			Assert.Equal(1, _subscribeProjectionHandler.HandledMessages.Count);
			Assert.Equal(0, _subscribeProjectionHandler.HandledMessages[0].FromPosition.Position.CommitPosition);
			Assert.Equal(-1, _subscribeProjectionHandler.HandledMessages[0].FromPosition.Position.PreparePosition);
		}
	}
}
