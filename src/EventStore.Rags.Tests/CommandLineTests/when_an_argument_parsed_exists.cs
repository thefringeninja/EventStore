using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Rags.Tests.CommandLineTests {
	public class when_an_argument_parsed_exists {
		[Fact]
		public void it_should_return_a_single_option_source() {
			IEnumerable<OptionSource> result = CommandLine.Parse<TestType>(new[] {"--name=bar"});
			Assert.Equal(1, result.Count());
			Assert.Equal("name", result.First().Name);
			Assert.False(result.First().IsTyped);
			Assert.Equal("bar", result.First().Value.ToString());
		}
	}
}
