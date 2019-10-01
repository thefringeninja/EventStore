using EventStore.Common.Options;
using EventStore.Core.Util;
using Xunit;

namespace EventStore.Core.Tests.Common.EventStoreOptionsTests.when_parsing {
	public class with_long_form_argument {
		[Fact]
		public void should_use_the_supplied_argument() {
			var args = new[] {"--log=~/customLogDirectory"};
			var testArgs = EventStoreOptions.Parse<TestArgs>(args, Opts.EnvPrefix);
			Assert.Equal("~/customLogDirectory", testArgs.Log);
		}

		[Fact]
		public void should_not_require_equals_in_method() {
			var args = new[] {"--log", "./customLogDirectory", "--run-projections", "all"};
			var testArgs = EventStoreOptions.Parse<TestArgs>(args, Opts.EnvPrefix);
			Assert.Equal("./customLogDirectory", testArgs.Log);
			Assert.Equal(ProjectionType.All, testArgs.RunProjections);
		}
	}
}
