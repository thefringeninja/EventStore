using System;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.core_projection {
	public class when_the_state_handler_with_configured_state_stream_does_process_an_event_the_projection_should :
		TestFixtureWithCoreProjectionStarted {
		protected override void Given() {
			_configureBuilderByQuerySource = source => {
				source.FromAll();
				source.AllEvents();
				source.SetResultStreamNameOption("state-stream");
				source.SetDefinesStateTransform();
			};
			AllWritesSucceed();
			NoOtherStreams();
		}

		protected override void When() {
			//projection subscribes here
			_bus.Publish(
				EventReaderSubscriptionMessage.CommittedEventReceived.Sample(
					new ResolvedEvent(
						"/event_category/1", -1, "/event_category/1", -1, false, new TFPos(120, 110),
						Guid.NewGuid(), "handle_this_type", false, "data",
						"metadata"), _subscriptionId, 0));
		}

		[Fact]
		public void write_the_new_state_snapshot() {
			Assert.Equal(1, _writeEventHandler.HandledMessages.ToStream("state-stream").Count);

			var message = _writeEventHandler.HandledMessages.ToStream("state-stream")[0];
			var data = Helper.UTF8NoBom.GetString(message.Events[0].Data);
			Assert.Equal("data", data);
			Assert.Equal("state-stream", message.EventStreamId);
		}

		[Fact]
		public void emit_a_state_updated_event() {
			Assert.Equal(1, _writeEventHandler.HandledMessages.ToStream("state-stream").Count);

			var @event = _writeEventHandler.HandledMessages.ToStream("state-stream")[0].Events[0];
			Assert.Equal("Result", @event.EventType);
		}
	}
}
