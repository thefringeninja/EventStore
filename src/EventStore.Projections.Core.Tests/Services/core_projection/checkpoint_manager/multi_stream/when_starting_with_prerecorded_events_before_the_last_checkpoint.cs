using System.Linq;
using EventStore.Core.Messages;
using Xunit;
using EventStore.Projections.Core.Services;

namespace EventStore.Projections.Core.Tests.Services.core_projection.checkpoint_manager.multi_stream {
	public class when_starting_with_prerecorded_events_before_the_last_checkpoint :
		TestFixtureWithMultiStreamCheckpointManager {
		protected override void Given() {
			base.Given();
			ExistingEvent(
				"$projections-projection-checkpoint", ProjectionEventTypes.ProjectionCheckpoint,
				@"{""s"": {""a"": 0, ""b"": 1, ""c"": 0}}", "{}");
			ExistingEvent("a", "StreamCreated", "", "");
			ExistingEvent("b", "StreamCreated", "", "");
			ExistingEvent("c", "StreamCreated", "", "");
			ExistingEvent("d", "StreamCreated", "", "");

			ExistingEvent("a", "Event", "", @"{""data"":""a""");
			ExistingEvent("b", "Event", "bb", @"{""data"":""b""");
			ExistingEvent("c", "$>", "{$o:\"org\"}", @"1@d");
			ExistingEvent("d", "Event", "dd", @"{""data"":""d""");

			// Lots of pre-recorded events before the checkpoint.
			for (int i = 0; i < 1000; i++) {
				ExistingEvent(
					"$projections-projection-order", "$>", @"{""s"": {""a"": 0, ""b"": 0, ""c"": 0}}", "0@c");
			}

			// Pre-recorded event at checkpoint 
			ExistingEvent(
				"$projections-projection-order", "$>", @"{""s"": {""a"": 0, ""b"": 1, ""c"": 0}}", "1@b");
		}

		protected override void When() {
			base.When();
			_checkpointReader.BeginLoadState();
			var checkpointLoaded =
				Consumer.HandledMessages.OfType<CoreProjectionProcessingMessage.CheckpointLoaded>().First();
			_checkpointWriter.StartFrom(checkpointLoaded.CheckpointTag, checkpointLoaded.CheckpointEventNumber);
			_manager.BeginLoadPrerecordedEvents(checkpointLoaded.CheckpointTag);
		}

		[Fact]
		public void stops_reading_prerecorded_events_after_found_checkpoint() {
			Assert.Equal(1,
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsBackward>()
					.Count(_ => _.EventStreamId == "$projections-projection-order"));
		}
	}
}
