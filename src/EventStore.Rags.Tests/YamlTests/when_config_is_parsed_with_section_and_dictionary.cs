using System;
using Xunit;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace EventStore.Rags.Tests.YamlTests {
	public class when_config_is_parsed_with_section_and_dictionary {
		[Fact]
		public void it_should_return_the_options_from_the_config_file() {
			var result = Yaml.FromFile(Path.Combine(Environment.CurrentDirectory,
				"YamlTests", "config_with_section_and_dictionary.yaml"), "Section");
			Assert.Single(result);
			Assert.Equal("Roles", result.First().Name);
			Assert.True(result.First().IsTyped);
			var dictionary = result.First().Value as Dictionary<string, string>;

			Assert.Equal(new Dictionary<string, string> {
					{"accounting", "$admins"},
					{"it-experts", "$experts"}
				},
				dictionary);
		}
	}
}
