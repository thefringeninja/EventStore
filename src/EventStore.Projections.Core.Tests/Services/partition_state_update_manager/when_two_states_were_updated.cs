using System.Linq;
using System.Text;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state_update_manager {
	public class when_two_states_were_updated {
		private PartitionStateUpdateManager _updateManager;
		private readonly CheckpointTag _zero = CheckpointTag.FromPosition(0, 100, 50);
		private readonly CheckpointTag _one = CheckpointTag.FromPosition(0, 200, 150);
		private readonly CheckpointTag _two = CheckpointTag.FromPosition(0, 300, 250);
		private readonly CheckpointTag _three = CheckpointTag.FromPosition(0, 400, 350);

		public when_two_states_were_updated() {
			_updateManager = new PartitionStateUpdateManager(ProjectionNamesBuilder.CreateForTest("projection"));
			_updateManager.StateUpdated("partition1", new PartitionState("{\"state\":1}", null, _one), _zero);
			_updateManager.StateUpdated("partition2", new PartitionState("{\"state\":2}", null, _two), _zero);
		}

		[Fact]
		public void handles_state_updated_for_the_same_partition() {
			_updateManager.StateUpdated("partition1", new PartitionState("{\"state\":0}", null, _three), _two);
		}

		[Fact]
		public void handles_state_updated_for_another_partition() {
			_updateManager.StateUpdated("partition3", new PartitionState("{\"state\":0}", null, _three), _two);
		}

		[Fact]
		public void emit_events_writes_both_state_updated_event() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			Assert.Equal(2, (eventWriter.Writes.SelectMany(write => write)).Count());
		}

		[Fact]
		public void emit_events_writes_to_correct_streams() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			var events = eventWriter.Writes.SelectMany(write => write).ToArray();
			Assert.True(events.Any((v => "$projections-projection-partition1-checkpoint" == v.StreamId)));
			Assert.True(events.Any((v => "$projections-projection-partition2-checkpoint" == v.StreamId)));
		}

		[Fact]
		public void emit_events_writes_correct_state_data() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			var events = eventWriter.Writes.SelectMany(write => write).ToArray();
			var event1 = events.Single(v => "$projections-projection-partition1-checkpoint" == v.StreamId);
			var event2 = events.Single(v => "$projections-projection-partition2-checkpoint" == v.StreamId);

			Assert.Equal("[{\"state\":1}]", event1.Data);
			Assert.Equal("[{\"state\":2}]", event2.Data);
		}

		[Fact]
		public void emit_events_writes_event_with_correct_caused_by_tag() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			var events = eventWriter.Writes.SelectMany(write => write).ToArray();
			var event1 = events.Single(v => "$projections-projection-partition1-checkpoint" == v.StreamId);
			var event2 = events.Single(v => "$projections-projection-partition2-checkpoint" == v.StreamId);
			Assert.Equal(_one, event1.CausedByTag);
			Assert.Equal(_two, event2.CausedByTag);
		}

		[Fact]
		public void emit_events_writes_event_with_correct_expected_tag() {
			var eventWriter = new FakeEventWriter();
			_updateManager.EmitEvents(eventWriter);
			var events = eventWriter.Writes.SelectMany(write => write).ToArray();
			var event1 = events.Single(v => "$projections-projection-partition1-checkpoint" == v.StreamId);
			var event2 = events.Single(v => "$projections-projection-partition2-checkpoint" == v.StreamId);
			Assert.Equal(_zero, event1.ExpectedTag);
			Assert.Equal(_zero, event2.ExpectedTag);
		}
	}
}
