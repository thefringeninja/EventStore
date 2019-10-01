using System;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.event_filter {
	public class empty_event_filter : TestFixtureWithEventFilter {
		[Fact]
		public void cannot_be_built() {
			Assert.IsAssignableFrom(typeof(InvalidOperationException), _exception);
		}
	}
}
