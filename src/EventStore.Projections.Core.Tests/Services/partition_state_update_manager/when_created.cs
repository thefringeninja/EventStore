using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state_update_manager {
	public class when_created {
		private PartitionStateUpdateManager _updateManager;

		public when_created() {
			_updateManager = new PartitionStateUpdateManager(ProjectionNamesBuilder.CreateForTest("projection"));
		}

		[Fact]
		public void handles_state_updated() {
			_updateManager.StateUpdated("partition",
				new PartitionState("state", null, CheckpointTag.FromPosition(0, 100, 50)),
				CheckpointTag.FromPosition(0, 200, 150));
		}

		[Fact]
		public void emit_events_does_not_write_any_events() {
			_updateManager.EmitEvents(new FakeEventWriter());
		}

		public class FakeEventWriter : IEventWriter {
			public void ValidateOrderAndEmitEvents(EmittedEventEnvelope[] events) {
				throw new Exception("Should not write any events");
			}
		}
	}
}
