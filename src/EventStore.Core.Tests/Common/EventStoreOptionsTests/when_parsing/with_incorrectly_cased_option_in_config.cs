using EventStore.Common.Options;
using EventStore.Core.Util;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EventStore.Core.Tests.Helpers;

namespace EventStore.Core.Tests.Common.EventStoreOptionsTests.when_parsing {
	public class with_incorrectly_cased_option_in_config {
		[Fact]
		public void should_be_able_to_parse_the_option_ignoring_casing() {
			var configFile =
				HelperExtensions.GetFilePathFromAssembly("TestConfigs/test_config_with_incorrectly_cased_option.yaml");
			var args = new string[] {"-config", configFile};
			var options = EventStoreOptions.Parse<TestArgs>(args, Opts.EnvPrefix);
			Assert.Equal("~/gesLogs", options.Log);
			Assert.Equal(ProjectionType.All, options.RunProjections);
		}
	}
}
