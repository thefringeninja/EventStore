using System;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_command_reader {
	public class when_creating : TestFixtureWithExistingEvents {
		private ProjectionCoreServiceCommandReader _commandReader;
		private Exception _exception;

		public when_creating() {
			_exception = null;
			try {
				_commandReader = new ProjectionCoreServiceCommandReader(
					_bus,
					_ioDispatcher,
					Guid.NewGuid().ToString("N"));
			} catch (Exception ex) {
				_exception = ex;
			}
		}

		[Fact]
		public void does_not_throw() {
			Assert.Null(_exception);
		}

		[Fact]
		public void it_can_be_created() {
			Assert.NotNull(_commandReader);
		}
	}
}
