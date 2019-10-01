using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using System.Collections;
using System.Collections.Generic;
using EventStore.Core.Bus;
using EventStore.Core.Helpers;
using EventStore.Projections.Core.Common;

namespace EventStore.Projections.Core.Tests.Services.core_projection.projection_checkpoint {

	public class when_emitting_events_with_maximum_allowed_writes_in_flight_set : TestFixtureWithExistingEvents {
		public static IEnumerable<object[]> TestCases()
			=> Enumerable.Range(1, 3).Select(x => new object[] {x});

		protected override void Given() {
			AllWritesQueueUp();
			AllWritesToSucceed("$$stream1");
			AllWritesToSucceed("$$stream2");
			AllWritesToSucceed("$$stream3");
			NoOtherStreams();
		}

		[Theory, MemberData(nameof(TestCases))]
		public void should_have_the_same_number_writes_in_flight_as_configured(int maximumNumberOfAllowedWritesInFlight) {
			using var fixture = new Fixture(maximumNumberOfAllowedWritesInFlight, _bus, _ioDispatcher);
			var writeEvents =
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.ExceptOfEventType(SystemEventTypes.StreamMetadata);
			Assert.Equal(maximumNumberOfAllowedWritesInFlight, writeEvents.Count());
		}

		class Fixture : IDisposable {
			private readonly ProjectionCheckpoint _checkpoint;
			private readonly TestCheckpointManagerMessageHandler _readyHandler;
			public Fixture(int maximumNumberOfAllowedWritesInFlight, IPublisher bus, IODispatcher ioDispatcher) {
				_readyHandler = new TestCheckpointManagerMessageHandler();
				_checkpoint = new ProjectionCheckpoint(
					bus, ioDispatcher, new ProjectionVersion(1, 0, 0), null, _readyHandler,
					CheckpointTag.FromPosition(0, 100, 50), new TransactionFilePositionTagger(0), 250,
					maximumNumberOfAllowedWritesInFlight);
				_checkpoint.Start();
				_checkpoint.ValidateOrderAndEmitEvents(
					new[] {
						new EmittedEventEnvelope(
							new EmittedDataEvent(
								"stream1", Guid.NewGuid(), "type1", true, "data1", null,
								CheckpointTag.FromPosition(0, 120, 110), null)),
						new EmittedEventEnvelope(
							new EmittedDataEvent(
								"stream2", Guid.NewGuid(), "type2", true, "data2", null,
								CheckpointTag.FromPosition(0, 120, 110), null)),
						new EmittedEventEnvelope(
							new EmittedDataEvent(
								"stream3", Guid.NewGuid(), "type3", true, "data3", null,
								CheckpointTag.FromPosition(0, 120, 110), null)),
					});

			}

			public void Dispose() => _checkpoint?.Dispose();
		} 
	}

	public class
		when_emitting_events_with_maximum_allowed_writes_in_flight_set_to_unlimited : TestFixtureWithExistingEvents {
		private ProjectionCheckpoint _checkpoint;
		private TestCheckpointManagerMessageHandler _readyHandler;

		protected override void Given() {
			AllWritesQueueUp();
			AllWritesToSucceed("$$stream1");
			AllWritesToSucceed("$$stream2");
			AllWritesToSucceed("$$stream3");
			NoOtherStreams();
		}

		public when_emitting_events_with_maximum_allowed_writes_in_flight_set_to_unlimited() {
			_readyHandler = new TestCheckpointManagerMessageHandler();
			_checkpoint = new ProjectionCheckpoint(
				_bus, _ioDispatcher, new ProjectionVersion(1, 0, 0), null, _readyHandler,
				CheckpointTag.FromPosition(0, 100, 50), new TransactionFilePositionTagger(0), 250,
				AllowedWritesInFlight.Unbounded);
			_checkpoint.Start();
			_checkpoint.ValidateOrderAndEmitEvents(
				new[] {
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream1", Guid.NewGuid(), "type1", true, "data1", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream2", Guid.NewGuid(), "type2", true, "data2", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
					new EmittedEventEnvelope(
						new EmittedDataEvent(
							"stream3", Guid.NewGuid(), "type3", true, "data3", null,
							CheckpointTag.FromPosition(0, 120, 110), null)),
				});
		}

		[Fact]
		public void should_have_as_many_writes_in_flight_as_requested() {
			var writeEvents =
				Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>()
					.ExceptOfEventType(SystemEventTypes.StreamMetadata);
			Assert.Equal(3, writeEvents.Count());
		}
	}
}
