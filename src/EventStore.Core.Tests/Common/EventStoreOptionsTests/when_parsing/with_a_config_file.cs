using EventStore.Common.Options;
using EventStore.Core.Util;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.Core.Tests.Helpers;

namespace EventStore.Core.Tests.Common.EventStoreOptionsTests.when_parsing {
	public class with_a_config_file {
		[Fact]
		public void should_use_the_config_file_value() {
			var configFile = HelperExtensions.GetFilePathFromAssembly("TestConfigs/test_config.yaml");
			var args = new string[] {"-config", configFile};
			var testArgs = EventStoreOptions.Parse<TestArgs>(args, Opts.EnvPrefix);
			Assert.Equal("~/logDirectoryFromConfigFile", testArgs.Log);
		}
	}
}
