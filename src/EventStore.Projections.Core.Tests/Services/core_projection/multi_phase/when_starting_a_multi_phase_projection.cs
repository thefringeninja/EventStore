using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.multi_phase {
    public class when_starting_a_multi_phase_projection : specification_with_multi_phase_core_projection {
		protected override void When() {
			_coreProjection.Start();
		}

		[Fact]
		public void it_starts() {
		}
	}
}
