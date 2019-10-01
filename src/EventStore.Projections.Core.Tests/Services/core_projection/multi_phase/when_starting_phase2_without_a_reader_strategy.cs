using System.Linq;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.multi_phase {
    public class when_starting_phase2_without_a_reader_strategy : specification_with_multi_phase_core_projection {
		protected override FakeReaderStrategy GivenPhase2ReaderStrategy() {
			return null;
		}

		protected override void When() {
			_coreProjection.Start();
			Phase1.Complete();
		}

		[Fact]
		public void initializes_phase2() {
			Assert.True(Phase2.InitializedFromCheckpoint);
		}

		[Fact]
		public void updates_checkpoint_tag_phase() {
			Assert.Equal(1, _coreProjection.LastProcessedEventPosition.Phase);
		}

		[Fact]
		public void starts_processing_phase2() {
			Assert.Equal(1, Phase2.ProcessEventInvoked);
		}
	}
}
