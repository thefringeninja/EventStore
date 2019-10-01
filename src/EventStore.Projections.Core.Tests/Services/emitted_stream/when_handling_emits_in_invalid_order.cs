using System;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream {
	public class when_handling_emits_in_invalid_order : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			ExistingEvent("test_stream", "type", @"{""c"": 100, ""p"": 50}", "data");
		}

		public when_handling_emits_in_invalid_order() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_stream = new EmittedStream(
				"test_stream",
				new EmittedStream.WriterConfiguration(new EmittedStreamsWriter(_ioDispatcher),
					new EmittedStream.WriterConfiguration.StreamMetadata(), null, maxWriteBatchLength: 50),
				new ProjectionVersion(1, 0, 0), new TransactionFilePositionTagger(0),
				CheckpointTag.FromPosition(0, 40, 30),
				_bus, _ioDispatcher, _readyHandler);
			_stream.Start();
			_stream.EmitEvents(
				new[] {
					new EmittedDataEvent(
						"test_stream", Guid.NewGuid(), "type", true, "data", null,
						CheckpointTag.FromPosition(0, 100, 90), null)
				});
		}

		[Fact]
		public void throws_if_position_is_prior_to_the_last_event_position() {
			Assert.Throws<InvalidOperationException>(() => {
				_stream.EmitEvents(
					new[] {
						new EmittedDataEvent(
							"test_stream", Guid.NewGuid(), "type", true, "data", null,
							CheckpointTag.FromPosition(0, 80, 70), null)
					});
			});
		}
	}
}
