using System;
using Xunit;
using System.IO;
using System.Linq;

namespace EventStore.Rags.Tests.YamlTests {
	public class when_config_is_parsed {
		[Fact]
		public void it_should_return_the_options_from_the_config_file() {
			var result = Yaml.FromFile(Path.Combine(Environment.CurrentDirectory,
				"YamlTests", "valid_config.yaml"));
			Assert.Single(result);
			Assert.Equal("Name", result.First().Name);
			Assert.False(result.First().IsTyped);
			Assert.Equal("foo", result.First().Value);
		}
	}
}
