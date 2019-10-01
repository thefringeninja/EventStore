using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream.another_epoch {
	public class
		when_handling_emits_with_previously_written_events_in_different_epochs : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;
		private long _1;
		private long _2;
		private long _3;

		protected override void Given() {
			AllWritesQueueUp();
			//NOTE: it is possible for a batch of events to be partially written if it contains links 
			ExistingEvent("test_stream", "type1", @"{""v"": 1, ""c"": 100, ""p"": 50}", "data");
			ExistingEvent("test_stream", "type1", @"{""v"": 2, ""c"": 100, ""p"": 50}", "data");
		}

		private EmittedEvent[] CreateEventBatch() {
			return new EmittedEvent[] {
				new EmittedDataEvent(
					(string)"test_stream", Guid.NewGuid(), (string)"type1", (bool)true,
					(string)"data", (ExtraMetaData)null, CheckpointTag.FromPosition(0, 100, 50), (CheckpointTag)null,
					v => _1 = v),
				new EmittedDataEvent(
					(string)"test_stream", Guid.NewGuid(), (string)"type2", (bool)true,
					(string)"data", (ExtraMetaData)null, CheckpointTag.FromPosition(0, 100, 50), (CheckpointTag)null,
					v => _2 = v),
				new EmittedDataEvent(
					(string)"test_stream", Guid.NewGuid(), (string)"type3", (bool)true,
					(string)"data", (ExtraMetaData)null, CheckpointTag.FromPosition(0, 100, 50), (CheckpointTag)null,
					v => _3 = v)
			};
		}

		public when_handling_emits_with_previously_written_events_in_different_epochs() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_stream = new EmittedStream(
				"test_stream",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, maxWriteBatchLength: 50),
				new ProjectionVersion(1, 2, 2), new TransactionFilePositionTagger(0),
				CheckpointTag.FromPosition(0, 20, 10),
				_bus, _ioDispatcher, _readyHandler);
			_stream.Start();
			_stream.EmitEvents(CreateEventBatch());
			OneWriteCompletes();
		}

		[Fact]
		public void publishes_all_events() {
			var writtenEvents =
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().SelectMany(v => v.Events).ToArray();
			Assert.Equal(2, writtenEvents.Length);
			Assert.Equal("type2", writtenEvents[0].EventType);
			Assert.Equal("type3", writtenEvents[1].EventType);
		}

		[Fact]
		public void updates_stream_metadata() {
			var writes =
				HandledMessages.OfType<ClientMessage.WriteEvents>()
					.OfEventType(SystemEventTypes.StreamMetadata)
					.ToArray();
			Assert.Equal(0, writes.Length);
		}


		[Fact]
		public void reports_correct_event_numbers() {
			Assert.Equal(1, _1);
			Assert.Equal(2, _2);
			Assert.Equal(3, _3);
		}
	}
}
