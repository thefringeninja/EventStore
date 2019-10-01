using System;
using System.Text;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Standard;
using Newtonsoft.Json.Linq;
using Xunit;
using ResolvedEvent = EventStore.Projections.Core.Services.Processing.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.handlers {
	public static class categorize_events_by_correlation_id {
		public class when_handling_simple_event {
			private ByCorrelationId _handler;
			private string _state;
			private EmittedEventEnvelope[] _emittedEvents;
			private bool _result;
			private DateTime _dateTime;

			public when_handling_simple_event() {
				_handler = new ByCorrelationId("", Console.WriteLine);
				_handler.Initialize();
				_dateTime = DateTime.UtcNow;
				var dataBytes = Encoding.ASCII.GetBytes("{}");
				var metadataBytes = Encoding.ASCII.GetBytes("{\"$correlationId\":\"testing1\"}");

				string sharedState;
				_result = _handler.ProcessEvent(
					"", CheckpointTag.FromPosition(0, 200, 150), null,
					new ResolvedEvent(
						"cat1-stream1", 10, "cat1-stream1", 10, false, new TFPos(200, 150), new TFPos(200, 150),
						Guid.NewGuid(),
						"event_type", true, dataBytes, metadataBytes, null, null, _dateTime), out _state,
					out sharedState, out _emittedEvents);
			}

			[Fact]
			public void result_is_true() {
				Assert.True(_result);
			}

			[Fact]
			public void state_stays_null() {
				Assert.Null(_state);
			}

			[Fact]
			public void emits_correct_link() {
				Assert.NotNull(_emittedEvents);
				Assert.Equal(1, _emittedEvents.Length);
				var @event = _emittedEvents[0].Event;
				Assert.Equal("$>", @event.EventType);
				Assert.Equal("$bc-testing1", @event.StreamId);
				Assert.Equal("10@cat1-stream1", @event.Data);

				string eventTimestampJson = null;
				var extraMetadata = @event.ExtraMetaData();
				foreach (var kvp in extraMetadata) {
					if (kvp.Key.Equals("$eventTimestamp")) {
						eventTimestampJson = kvp.Value;
					}
				}

				Assert.NotNull(eventTimestampJson);
				Assert.Equal("\"" + _dateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffffZ") + "\"", eventTimestampJson);
			}
		}

		public class when_handling_link_to_event {
			private ByCorrelationId _handler;
			private string _state;
			private EmittedEventEnvelope[] _emittedEvents;
			private bool _result;
			private DateTime _dateTime;
			private Guid _eventId;

			public when_handling_link_to_event() {
				_handler = new ByCorrelationId("", Console.WriteLine);
				_handler.Initialize();
				_dateTime = DateTime.UtcNow;
				string sharedState;

				var dataBytes = Encoding.ASCII.GetBytes("10@cat1-stream1");
				var metadataBytes =
					Encoding.ASCII.GetBytes("{\"$correlationId\":\"testing2\", \"$whatever\":\"hello\"}");
				var myEvent = new ResolvedEvent("cat2-stream2", 20, "cat2-stream2", 20, true, new TFPos(200, 150),
					new TFPos(200, 150), Guid.NewGuid(), "$>", true, dataBytes, metadataBytes, null, null, _dateTime);

				_eventId = myEvent.EventId;
				_result = _handler.ProcessEvent(
					"", CheckpointTag.FromPosition(0, 200, 150), null,
					myEvent, out _state, out sharedState, out _emittedEvents);
			}

			[Fact]
			public void result_is_true() {
				Assert.True(_result);
			}

			[Fact]
			public void state_stays_null() {
				Assert.Null(_state);
			}

			[Fact]
			public void emits_correct_link() {
				Assert.NotNull(_emittedEvents);
				Assert.Equal(1, _emittedEvents.Length);
				var @event = _emittedEvents[0].Event;
				Assert.Equal("$>", @event.EventType);
				Assert.Equal("$bc-testing2", @event.StreamId);
				Assert.Equal("10@cat1-stream1", @event.Data);

				string eventTimestampJson = null;
				string linkJson = null;
				var extraMetadata = @event.ExtraMetaData();
				foreach (var kvp in extraMetadata) {
					switch (kvp.Key) {
						case "$eventTimestamp":
							eventTimestampJson = kvp.Value;
							break;
						case "$link":
							linkJson = kvp.Value;
							break;
					}
				}

				Assert.NotNull(eventTimestampJson);
				Assert.Equal("\"" + _dateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffffZ") + "\"", eventTimestampJson);

				//the link's metadata should be copied to $link.metadata and id to $link.eventId
				Assert.NotNull(linkJson);
				var link = JObject.Parse(linkJson);
				Assert.Equal(link.GetValue("eventId").ToObject<string>(), _eventId.ToString());

				var linkMetadata = (JObject)link.GetValue("metadata");
				Assert.Equal(linkMetadata.GetValue("$correlationId").ToObject<string>(), "testing2");
				Assert.Equal(linkMetadata.GetValue("$whatever").ToObject<string>(), "hello");
			}
		}

		public class when_handling_non_json_event {
			private ByCorrelationId _handler;
			private string _state;
			private EmittedEventEnvelope[] _emittedEvents;
			private bool _result;

			public when_handling_non_json_event() {
				_handler = new ByCorrelationId("", Console.WriteLine);
				_handler.Initialize();
				string sharedState;
				_result = _handler.ProcessEvent(
					"", CheckpointTag.FromPosition(0, 200, 150), null,
					new ResolvedEvent(
						"cat1-stream1", 10, "cat1-stream1", 10, false, new TFPos(200, 150), Guid.NewGuid(),
						"event_type", false, "non_json_data", "non_json_metadata"), out _state, out sharedState,
					out _emittedEvents);
			}

			[Fact]
			public void result_is_false() {
				Assert.False(_result);
			}

			[Fact]
			public void state_stays_null() {
				Assert.Null(_state);
			}

			[Fact]
			public void does_not_emit_link() {
				Assert.Null(_emittedEvents);
			}
		}

		public class when_handling_json_event_with_no_correlation_id {
			private ByCorrelationId _handler;
			private string _state;
			private EmittedEventEnvelope[] _emittedEvents;
			private bool _result;

			public when_handling_json_event_with_no_correlation_id() {
				_handler = new ByCorrelationId("", Console.WriteLine);
				_handler.Initialize();
				string sharedState;
				_result = _handler.ProcessEvent(
					"", CheckpointTag.FromPosition(0, 200, 150), null,
					new ResolvedEvent(
						"cat1-stream1", 10, "cat1-stream1", 10, false, new TFPos(200, 150), Guid.NewGuid(),
						"event_type", true, "{}", "{}"), out _state, out sharedState, out _emittedEvents);
			}

			[Fact]
			public void result_is_false() {
				Assert.False(_result);
			}

			[Fact]
			public void state_stays_null() {
				Assert.Null(_state);
			}

			[Fact]
			public void does_not_emit_link() {
				Assert.Null(_emittedEvents);
			}
		}

		public class when_handling_json_event_with_non_json_metadata {
			private ByCorrelationId _handler;
			private string _state;
			private EmittedEventEnvelope[] _emittedEvents;
			private bool _result;

			public when_handling_json_event_with_non_json_metadata() {
				_handler = new ByCorrelationId("", Console.WriteLine);
				_handler.Initialize();
				string sharedState;
				_result = _handler.ProcessEvent(
					"", CheckpointTag.FromPosition(0, 200, 150), null,
					new ResolvedEvent(
						"cat1-stream1", 10, "cat1-stream1", 10, false, new TFPos(200, 150), Guid.NewGuid(),
						"event_type", true, "{}", "non_json_metadata"), out _state, out sharedState,
					out _emittedEvents);
			}

			[Fact]
			public void result_is_false() {
				Assert.False(_result);
			}

			[Fact]
			public void state_stays_null() {
				Assert.Null(_state);
			}

			[Fact]
			public void does_not_emit_link() {
				Assert.Null(_emittedEvents);
			}
		}

		public class with_custom_valid_correlation_id_property {
			private ByCorrelationId _handler;
			private string _state;
			private EmittedEventEnvelope[] _emittedEvents;
			private bool _result;
			private string source = "{\"correlationIdProperty\":\"$myCorrelationId\"}";

			public with_custom_valid_correlation_id_property() {
				_handler = new ByCorrelationId(source, Console.WriteLine);
				_handler.Initialize();
				string sharedState;
				_result = _handler.ProcessEvent(
					"", CheckpointTag.FromPosition(0, 200, 150), null,
					new ResolvedEvent(
						"cat1-stream1", 10, "cat1-stream1", 10, false, new TFPos(200, 150), Guid.NewGuid(),
						"event_type", true, "{}", "{\"$myCorrelationId\":\"testing1\"}"), out _state, out sharedState,
					out _emittedEvents);
			}

			[Fact]
			public void result_is_true() {
				Assert.True(_result);
			}

			[Fact]
			public void state_stays_null() {
				Assert.Null(_state);
			}

			[Fact]
			public void emits_correct_link() {
				Assert.NotNull(_emittedEvents);
				Assert.Equal(1, _emittedEvents.Length);
				var @event = _emittedEvents[0].Event;
				Assert.Equal("$>", @event.EventType);
				Assert.Equal("$bc-testing1", @event.StreamId);
				Assert.Equal("10@cat1-stream1", @event.Data);
			}
		}

		public class with_custom_invalid_correlation_id_property {
			private string source = "{\"thisisnotvalid\":\"$myCorrelationId\"}";

			[Fact]
			public void should_throw_invalid_operation_exception() {
				Assert.Throws<InvalidOperationException>(() => { new ByCorrelationId(source, Console.WriteLine); });
			}
		}
	}
}
