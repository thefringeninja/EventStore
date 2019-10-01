using System;
using System.Linq;
using EventStore.Core.Bus;
using EventStore.Core.Tests.Helpers;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection.checkpoint_manager;
using EventStore.Projections.Core.Tests.Services.core_projection.multi_phase;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.write_query_result_phase {
	namespace creating {
		public class when_creating {
			[Fact]
			public void it_can_be_created() {
				var coreProjection = new FakeCoreProjection();
				var stateCache = new PartitionStateCache();
				var bus = new InMemoryBus("test");
				var fakeCheckpointManager =
					new specification_with_multi_phase_core_projection.FakeCheckpointManager(bus, Guid.NewGuid());
				var fakeEmittedStreamsTracker =
					new specification_with_multi_phase_core_projection.FakeEmittedStreamsTracker();
				TestHelper.Consume(
					new WriteQueryResultProjectionProcessingPhase(
						bus,
						1,
						"result-stream",
						coreProjection,
						stateCache,
						fakeCheckpointManager,
						fakeCheckpointManager,
						fakeEmittedStreamsTracker));
			}
		}

		public abstract class specification_with_write_query_result_projection_processing_phase : IDisposable {
			protected WriteQueryResultProjectionProcessingPhase _phase;
			protected specification_with_multi_phase_core_projection.FakeCheckpointManager _checkpointManager;
			protected specification_with_multi_phase_core_projection.FakeEmittedStreamsTracker _emittedStreamsTracker;
			protected InMemoryBus _publisher;
			protected PartitionStateCache _stateCache;
			protected string _resultStreamName;
			protected FakeCoreProjection _coreProjection;

			public specification_with_write_query_result_projection_processing_phase() {
				_stateCache = GivenStateCache();
				_publisher = new InMemoryBus("test");
				_coreProjection = new FakeCoreProjection();
				_checkpointManager = new specification_with_multi_phase_core_projection.FakeCheckpointManager(
					_publisher, Guid.NewGuid());
				_emittedStreamsTracker = new specification_with_multi_phase_core_projection.FakeEmittedStreamsTracker();
				_resultStreamName = "result-stream";
				_phase = new WriteQueryResultProjectionProcessingPhase(
					_publisher,
					1,
					_resultStreamName,
					_coreProjection,
					_stateCache,
					_checkpointManager,
					_checkpointManager,
					_emittedStreamsTracker);
				When();
			}

			protected virtual PartitionStateCache GivenStateCache() {
				var stateCache = new PartitionStateCache();

				stateCache.CachePartitionState(
					"a", new PartitionState("{}", null, CheckpointTag.FromPhase(0, completed: false)));
				stateCache.CachePartitionState(
					"b", new PartitionState("{}", null, CheckpointTag.FromPhase(0, completed: false)));
				stateCache.CachePartitionState(
					"c", new PartitionState("{}", null, CheckpointTag.FromPhase(0, completed: false)));
				return stateCache;
			}

			protected abstract void When();


			public void Dispose() {
				_phase = null;
			}
		}

		public class when_created : specification_with_write_query_result_projection_processing_phase {
			protected override void When() {
			}

			[Fact]
			public void can_be_initialized_from_phase_checkpoint() {
				_phase.InitializeFromCheckpoint(CheckpointTag.FromPhase(1, completed: false));
			}

			[Fact]
			public void process_event_throws_invalid_operation_exception() {
				Assert.Throws<InvalidOperationException>(() => { _phase.ProcessEvent(); });
			}
		}

		public class when_subscribing : specification_with_write_query_result_projection_processing_phase {
			protected override void When() {
				_phase.Subscribe(CheckpointTag.FromPhase(1, completed: false), false);
			}

			[Fact]
			public void notifies_core_projection_with_subscribed() {
				Assert.Equal(1, _coreProjection.SubscribedInvoked);
			}
		}

		public class when_processing_event : specification_with_write_query_result_projection_processing_phase {
			protected override void When() {
				_phase.Subscribe(CheckpointTag.FromPhase(1, completed: false), false);
				_phase.SetProjectionState(PhaseState.Running);
				_phase.ProcessEvent();
			}

			[Fact]
			public void writes_query_results() {
				Assert.Equal(3, _checkpointManager.EmittedEvents.Count(v => v.Event.EventType == "Result"));
			}
		}

		public class
			when_completed_query_processing_event : specification_with_write_query_result_projection_processing_phase {
			protected override void When() {
				_phase.Subscribe(CheckpointTag.FromPhase(1, completed: false), false);
				_phase.SetProjectionState(PhaseState.Running);
				_phase.ProcessEvent();
				_phase.SetProjectionState(PhaseState.Stopped);
				_phase.ProcessEvent();
			}

			[Fact]
			public void writes_query_results_only_once() {
				Assert.Equal(3, _checkpointManager.EmittedEvents.Count(v => v.Event.EventType == "Result"));
			}
		}
	}
}
