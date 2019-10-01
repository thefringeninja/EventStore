using System.Text;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state_update_manager {
	public class when_state_updated {
		private PartitionStateUpdateManager _updateManager;
		private CheckpointTag _zero = CheckpointTag.FromPosition(0, 100, 50);
		private CheckpointTag _one = CheckpointTag.FromPosition(0, 200, 150);
		private CheckpointTag _two = CheckpointTag.FromPosition(0, 300, 250);

		public when_state_updated() {
			_updateManager = new PartitionStateUpdateManager(ProjectionNamesBuilder.CreateForTest("projection"));
			_updateManager.StateUpdated("partition", new PartitionState("{\"state\":1}", null, _one), _zero);
		}

		[Fact]
		public void handles_state_updated_for_the_same_partition() {
			_updateManager.StateUpdated("partition", new PartitionState("{\"state\":1}", null, _two), _one);
		}

		[Fact]
		public void handles_state_updated_for_another_partition() {
			_updateManager.StateUpdated("partition", new PartitionState("{\"state\":1}", null, _two), _one);
		}

		[Fact]
		public void emit_events_writes_single_state_updated_event() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			Assert.Equal(1, eventWriter.Writes.Count);
			Assert.Equal(1, eventWriter.Writes[0].Length);
		}

		[Fact]
		public void emit_events_writes_correct_state_data() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			EmittedEvent @event = eventWriter.Writes[0][0];
			Assert.Equal("[{\"state\":1}]", @event.Data);
		}

		[Fact]
		public void emit_events_writes_event_with_correct_caused_by_tag() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			EmittedEvent @event = eventWriter.Writes[0][0];
			Assert.Equal(_one, @event.CausedByTag);
		}

		[Fact]
		public void emit_events_writes_event_with_correct_expected_tag() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			EmittedEvent @event = eventWriter.Writes[0][0];
			Assert.Equal(_zero, @event.ExpectedTag);
		}
	}
}
