using System;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader {
	public class when_creating : TestFixtureWithExistingEvents {
		private ProjectionManagerResponseReader _commandReader;

		public when_creating() {
			_commandReader = new ProjectionManagerResponseReader(_bus, _ioDispatcher, 0);
		}

		[Fact]
		public void it_can_be_created() {
			Assert.NotNull(_commandReader);
		}
	}
}
