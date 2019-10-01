using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.projection_checkpoint {
	public class
		when_emitting_events_in_correct_order_the_started_projection_checkpoint : TestFixtureWithExistingEvents {
		private ProjectionCheckpoint _checkpoint;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			AllWritesQueueUp();
			AllWritesToSucceed("$$stream1");
			AllWritesToSucceed("$$stream2");
			AllWritesToSucceed("$$stream3");
			NoOtherStreams();
		}

		public when_emitting_events_in_correct_order_the_started_projection_checkpoint() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_checkpoint = new ProjectionCheckpoint(
				_bus, _ioDispatcher, new ProjectionVersion(1, 0, 0), null, _readyHandler,
				CheckpointTag.FromPosition(0, 100, 50), new TransactionFilePositionTagger(0), 250, 1);
			_checkpoint.Start();
			_checkpoint.ValidateOrderAndEmitEvents(
				new[] {
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream2", Guid.NewGuid(), "type1", true, "data2", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream3", Guid.NewGuid(), "type2", true, "data3", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream2", Guid.NewGuid(), "type3", true, "data4", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
				});
			_checkpoint.ValidateOrderAndEmitEvents(
				new[] {
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream1", Guid.NewGuid(), "type4", true, "data", null,
							CheckpointTag.FromPosition(0, 140, 130), null))
				});
			OneWriteCompletes(); //stream2
			OneWriteCompletes(); //stream3
		}

		[Fact]
		public void should_publish_write_events() {
			var writeEvents =
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.ExceptOfEventType(SystemEventTypes.StreamMetadata);
			Assert.Equal(4, writeEvents.Count());
		}

		[Fact]
		public void should_publish_write_events_to_correct_streams() {
			Assert.True(
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Any(v => v.EventStreamId == "stream1"));
			Assert.True(
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Any(v => v.EventStreamId == "stream2"));
			Assert.True(
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Any(v => v.EventStreamId == "stream3"));
		}

		[Fact]
		public void should_group_events_to_the_same_stream_caused_by_the_same_event() {
			// this is important for the projection to be able to recover by CausedBy.  Unless we commit all the events
			// to the stream in a single transaction we can get into situation when only part of events CausedBy the same event
			// are present in a stream
			Assert.Equal(
				2,
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Single(v => v.EventStreamId == "stream2")
					.Events.Length);
		}

		[Fact]
		public void should_not_write_a_second_group_until_the_first_write_completes() {
			_checkpoint.ValidateOrderAndEmitEvents(
				new[] {
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream1", Guid.NewGuid(), "type", true, "data", null,
							CheckpointTag.FromPosition(0, 170, 160), null))
				});
			var writeRequests =
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Where(v => v.EventStreamId == "stream1");
			var writeEvents = writeRequests.Single();
			writeEvents.Envelope.ReplyWith(
				new ClientMessage.WriteEventsCompleted(writeEvents.CorrelationId, 0, 0, -1, -1));
			Assert.Equal(2, writeRequests.Count());
		}
	}
}
