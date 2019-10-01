using System;
using System.Text;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.result_emitter {
	public static class result_emitter {
		public class when_creating {
			private ProjectionNamesBuilder _namesBuilder;

			public when_creating() {
				_namesBuilder = ProjectionNamesBuilder.CreateForTest("projection");
			}

			[Fact]
			public void it_can_be_created() {
				new ResultEventEmitter(_namesBuilder);
			}

			[Fact]
			public void null_names_builder_throws_argument_null_exception() {
				Assert.Throws<ArgumentNullException>(() => { new ResultEventEmitter(null); });
			}
		}

		public class when_result_updated {
			private ProjectionNamesBuilder _namesBuilder;
			private ResultEventEmitter _re;
			private string _partition;
			private string _projection;
			private CheckpointTag _resultAt;
			private EmittedEventEnvelope[] _emittedEvents;
			private string _result;

			public when_result_updated() {
				Given();
				When();
			}

			private void Given() {
				_projection = "projection";
				_resultAt = CheckpointTag.FromPosition(0, 100, 50);
				_partition = "partition";
				_result = "{\"result\":1}";
				_namesBuilder = ProjectionNamesBuilder.CreateForTest(_projection);
				_re = new ResultEventEmitter(_namesBuilder);
			}

			private void When() {
				_emittedEvents = _re.ResultUpdated(_partition, _result, _resultAt);
			}

			[Fact]
			public void emits_result_event() {
				Assert.NotNull(_emittedEvents);
				Assert.Equal(2, _emittedEvents.Length);
				var @event = _emittedEvents[0];
				var link = _emittedEvents[1].Event;

				Assert.Equal("Result", @event.Event.EventType);
				Assert.Equal(_result, @event.Event.Data);
				Assert.Equal("$projections-projection-partition-result", @event.Event.StreamId);
				Assert.Equal(_resultAt, @event.Event.CausedByTag);
				Assert.Null(@event.Event.ExpectedTag);

				Assert.Equal("$>", link.EventType);
				((EmittedLinkTo)link).SetTargetEventNumber(1);
				Assert.Equal("1@$projections-projection-partition-result", link.Data);
				Assert.Equal("$projections-projection-result", link.StreamId);
				Assert.Equal(_resultAt, link.CausedByTag);
				Assert.Null(link.ExpectedTag);
			}
		}

		public class when_result_removed {
			private ProjectionNamesBuilder _namesBuilder;
			private ResultEventEmitter _re;
			private string _partition;
			private string _projection;
			private CheckpointTag _resultAt;
			private EmittedEventEnvelope[] _emittedEvents;

			public when_result_removed() {
				Given();
				When();
			}

			private void Given() {
				_projection = "projection";
				_resultAt = CheckpointTag.FromPosition(0, 100, 50);
				_partition = "partition";
				_namesBuilder = ProjectionNamesBuilder.CreateForTest(_projection);
				_re = new ResultEventEmitter(_namesBuilder);
			}

			private void When() {
				_emittedEvents = _re.ResultUpdated(_partition, null, _resultAt);
			}

			[Fact]
			public void emits_result_event() {
				Assert.NotNull(_emittedEvents);
				Assert.Equal(2, _emittedEvents.Length);
				var @event = _emittedEvents[0];
				var link = _emittedEvents[1].Event;

				Assert.Equal("ResultRemoved", @event.Event.EventType);
				Assert.Null(@event.Event.Data);
				Assert.Equal("$projections-projection-partition-result", @event.Event.StreamId);
				Assert.Equal(_resultAt, @event.Event.CausedByTag);
				Assert.Null(@event.Event.ExpectedTag);

				Assert.Equal("$>", link.EventType);
				((EmittedLinkTo)link).SetTargetEventNumber(1);
				Assert.Equal("1@$projections-projection-partition-result", link.Data);
				Assert.Equal("$projections-projection-result", link.StreamId);
				Assert.Equal(_resultAt, link.CausedByTag);
				Assert.Null(link.ExpectedTag);
			}
		}

		public class when_result_updated_on_root_partition {
			private ProjectionNamesBuilder _namesBuilder;
			private ResultEventEmitter _re;
			private string _partition;
			private string _projection;
			private CheckpointTag _resultAt;
			private EmittedEventEnvelope[] _emittedEvents;
			private string _result;

			public when_result_updated_on_root_partition() {
				Given();
				When();
			}

			private void Given() {
				_projection = "projection";
				_resultAt = CheckpointTag.FromPosition(0, 100, 50);
				_partition = "";
				_result = "{\"result\":1}";
				_namesBuilder = ProjectionNamesBuilder.CreateForTest(_projection);
				_re = new ResultEventEmitter(_namesBuilder);
			}

			private void When() {
				_emittedEvents = _re.ResultUpdated(_partition, _result, _resultAt);
			}

			[Fact]
			public void emits_result_event() {
				Assert.NotNull(_emittedEvents);
				Assert.Equal(1, _emittedEvents.Length);
				var @event = _emittedEvents[0].Event;

				Assert.Equal("Result", @event.EventType);
				Assert.Equal(_result, @event.Data);
				Assert.Equal("$projections-projection-result", @event.StreamId);
				Assert.Equal(_resultAt, @event.CausedByTag);
				Assert.Null(@event.ExpectedTag);
			}
		}
	}
}
