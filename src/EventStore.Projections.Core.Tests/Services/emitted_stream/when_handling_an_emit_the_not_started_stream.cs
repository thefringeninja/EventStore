using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream {
	public class when_handling_an_emit_the_not_started_stream : TestFixtureWithExistingEvents {
		private EmittedStream _stream;

		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			base.Given();
			NoStream("test");
		}

		public when_handling_an_emit_the_not_started_stream() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			;
			_stream = new EmittedStream(
				"test",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, 50), new ProjectionVersion(1, 0, 0),
				new TransactionFilePositionTagger(0), CheckpointTag.FromPosition(0, 0, -1), _bus, _ioDispatcher,
				_readyHandler);
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test", Guid.NewGuid(), "type", true, "data", null, CheckpointTag.FromPosition(0, 200, 150),
						null)
				});
		}

		[Fact]
		public void does_not_publish_write_events() {
			Assert.Equal(0, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
		}

		[Fact]
		public void publishes_write_events_when_started() {
			_stream.Start();
			Assert.Equal(1, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
		}
	}
}
