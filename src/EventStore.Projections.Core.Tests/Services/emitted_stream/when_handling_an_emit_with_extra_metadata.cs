using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Tests.Helpers;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Newtonsoft.Json.Linq;
using Xunit;
using TestFixtureWithExistingEvents =
	EventStore.Projections.Core.Tests.Services.core_projection.TestFixtureWithExistingEvents;

namespace EventStore.Projections.Core.Tests.Services.emitted_stream {
	public class when_handling_an_emit_with_extra_metadata : TestFixtureWithExistingEvents {
		private EmittedStream _stream;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			AllWritesQueueUp();
			ExistingEvent("test_stream", "type", @"{""c"": 100, ""p"": 50}", "data");
		}

		public when_handling_an_emit_with_extra_metadata() {
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
						"test_stream", Guid.NewGuid(), "type", true, "data",
						new ExtraMetaData(new Dictionary<string, string> {{"a", "1"}, {"b", "{}"}}),
						CheckpointTag.FromPosition(0, 200, 150), null)
				});
		}

		[Fact]
		public void publishes_not_yet_published_events() {
			Assert.Equal(1, Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count());
		}

		[Fact]
		public void combines_checkpoint_tag_with_extra_metadata() {
			var writeEvent = Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Single();

			Assert.Equal(1, writeEvent.Events.Length);
			var @event = writeEvent.Events[0];
			var metadata = Helper.UTF8NoBom.GetString(@event.Metadata).ParseJson<JObject>();

			HelperExtensions.AssertJson(new {a = 1, b = new { }}, metadata);
			var checkpoint = @event.Metadata.ParseCheckpointTagJson();
			Assert.Equal(CheckpointTag.FromPosition(0, 200, 150), checkpoint);
		}
	}
}
