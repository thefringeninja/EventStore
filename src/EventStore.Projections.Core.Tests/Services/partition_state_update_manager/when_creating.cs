using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.partition_state_update_manager {
	public class when_creating {
		[Fact]
		public void no_exceptions_are_thrown() {
			new PartitionStateUpdateManager(ProjectionNamesBuilder.CreateForTest("projection"));
		}

		[Fact]
		public void null_naming_builder_throws_argument_null_exception() {
			Assert.Throws<ArgumentNullException>(() => { new PartitionStateUpdateManager(null); });
		}
	}
}
