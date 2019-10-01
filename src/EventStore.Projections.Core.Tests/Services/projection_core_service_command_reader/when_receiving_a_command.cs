using System;
using System.Collections.Generic;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_command_reader {
	public class when_receiving_a_command : specification_with_projection_core_service_command_reader_started {
		protected override IEnumerable<WhenStep> When() {
			yield return
				CreateWriteEvent(
					"$projections-$" + _serviceId,
					"$start",
					"{\"id\":\"" + Guid.NewGuid().ToString("N") + "\"}",
					null,
					true);
		}

		[Fact]
		public void it_works() {
			Assert.NotEmpty(_serviceId);
		}
	}
}
