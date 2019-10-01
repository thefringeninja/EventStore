using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream.another_projection {
	public class
		when_handling_an_emit_with_expected_tag_the_started_in_recovery_stream : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			ExistingEvent("test_stream", "type", @"{""v"": ""2:3:4"", ""c"": 100, ""p"": 50}", "data");
		}

		public when_handling_an_emit_with_expected_tag_the_started_in_recovery_stream() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_stream = new EmittedStream(
				"test_stream",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, maxWriteBatchLength: 50),
				new ProjectionVersion(1, 2, 2), new TransactionFilePositionTagger(0),
				CheckpointTag.FromPosition(0, 0, -1),
				_bus, _ioDispatcher, _readyHandler);
			_stream.Start();
		}

		[Fact]
		public void fails_the_projection() {
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 100, 50), CheckpointTag.FromPosition(0, 40, 20))
				});
			Assert.Equal(0, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
			Assert.Equal(1, _readyHandler.HandledFailedMessages.Count());
		}
	}
}
