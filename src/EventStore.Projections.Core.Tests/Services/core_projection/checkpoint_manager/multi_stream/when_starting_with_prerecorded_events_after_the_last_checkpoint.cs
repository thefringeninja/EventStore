using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using EventStore.Projections.Core.Services;

namespace EventStore.Projections.Core.Tests.Services.core_projection.checkpoint_manager.multi_stream {
	public class when_starting_with_prerecorded_events_after_the_last_checkpoint :
		TestFixtureWithMultiStreamCheckpointManager {
		private readonly CheckpointTag _tag1 =
			CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"a", 0}, {"b", 0}, {"c", 1}});

		private readonly CheckpointTag _tag2 =
			CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"a", 1}, {"b", 0}, {"c", 1}});

		private readonly CheckpointTag _tag3 =
			CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"a", 1}, {"b", 1}, {"c", 1}});

		protected override void Given() {
			base.Given();
			ExistingEvent(
				"$projections-projection-checkpoint", ProjectionEventTypes.ProjectionCheckpoint,
				@"{""s"": {""a"": 0, ""b"": 0, ""c"": 0}}", "{}");
			ExistingEvent("a", "StreamCreated", "", "");
			ExistingEvent("b", "StreamCreated", "", "");
			ExistingEvent("c", "StreamCreated", "", "");
			ExistingEvent("d", "StreamCreated", "", "");

			ExistingEvent("a", "Event", "", @"{""data"":""a""");
			ExistingEvent("b", "Event", "bb", @"{""data"":""b""");
			ExistingEvent("c", "$>", "{$o:\"org\"}", @"1@d");
			ExistingEvent("d", "Event", "dd", @"{""data"":""d""");

			ExistingEvent(
				"$projections-projection-order", "$>", @"{""s"": {""a"": 0, ""b"": 0, ""c"": 0}}", "0@c");
			ExistingEvent(
				"$projections-projection-order", "$>", @"{""s"": {""a"": 0, ""b"": 0, ""c"": 1}}", "1@c");
			ExistingEvent(
				"$projections-projection-order", "$>", @"{""s"": {""a"": 1, ""b"": 0, ""c"": 1}}", "1@a");
			ExistingEvent(
				"$projections-projection-order", "$>", @"{""s"": {""a"": 1, ""b"": 1, ""c"": 1}}", "1@b");
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
		public void sends_correct_checkpoint_loaded_message() {
			Assert.Equal(1, _projection._checkpointLoadedMessages.Count);
			Assert.Equal(
				CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"a", 0}, {"b", 0}, {"c", 0}}),
				_projection._checkpointLoadedMessages.Single().CheckpointTag);
			Assert.Equal("{}", _projection._checkpointLoadedMessages.Single().CheckpointData);
		}

		[Fact]
		public void sends_correct_preprecoded_events_loaded_message() {
			Assert.Equal(1, _projection._prerecordedEventsLoadedMessages.Count);
			Assert.Equal(
				CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"a", 1}, {"b", 1}, {"c", 1}}),
				_projection._prerecordedEventsLoadedMessages.Single().CheckpointTag);
		}

		[Fact]
		public void sends_commited_event_received_messages_in_correct_order() {
			var messages = HandledMessages.OfType<EventReaderSubscriptionMessage.CommittedEventReceived>().ToList();
			Assert.Equal(3, messages.Count);

			var message1 = messages[0];
			var message2 = messages[1];
			var message3 = messages[2];

			Assert.Equal(_tag1, message1.CheckpointTag);
			Assert.Equal(_tag2, message2.CheckpointTag);
			Assert.Equal(_tag3, message3.CheckpointTag);
		}

		[Fact]
		public void sends_correct_commited_event_received_messages() {
			var messages = HandledMessages.OfType<EventReaderSubscriptionMessage.CommittedEventReceived>().ToList();
			Assert.Equal(3, messages.Count);

			var message1 = messages[0];
			var message2 = messages[1];
			var message3 = messages[2];

			Assert.Equal(@"{""data"":""d""", message1.Data.Data);
			Assert.Equal(@"{""data"":""a""", message2.Data.Data);
			Assert.Equal(@"{""data"":""b""", message3.Data.Data);

			Assert.Equal(@"dd", message1.Data.Metadata);
			Assert.Equal(@"", message2.Data.Metadata);
			Assert.Equal(@"bb", message3.Data.Metadata);

			Assert.Equal("{$o:\"org\"}", message1.Data.PositionMetadata);
			Assert.Null(message2.Data.PositionMetadata);
			Assert.Null(message3.Data.PositionMetadata);

			Assert.Equal("Event", message1.Data.EventType);
			Assert.Equal("Event", message2.Data.EventType);
			Assert.Equal("Event", message3.Data.EventType);

			Assert.Equal("c", message1.Data.PositionStreamId);
			Assert.Equal("a", message2.Data.PositionStreamId);
			Assert.Equal("b", message3.Data.PositionStreamId);

			Assert.Equal("d", message1.Data.EventStreamId);
			Assert.Equal("a", message2.Data.EventStreamId);
			Assert.Equal("b", message3.Data.EventStreamId);

			Assert.Equal(_projectionCorrelationId, message1.SubscriptionId);
			Assert.Equal(_projectionCorrelationId, message2.SubscriptionId);
			Assert.Equal(_projectionCorrelationId, message3.SubscriptionId);

			Assert.Equal(true, message1.Data.ResolvedLinkTo);
			Assert.False(message2.Data.ResolvedLinkTo);
			Assert.False(message3.Data.ResolvedLinkTo);
		}
	}
}
