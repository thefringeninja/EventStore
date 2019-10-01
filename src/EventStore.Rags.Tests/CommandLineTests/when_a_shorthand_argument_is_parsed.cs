using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Rags.Tests.CommandLineTests {
	public class when_a_shorthand_argument_is_parsed {
		[Fact]
		public void with_a_trailing_dash_should_return_the_symbol() {
			IEnumerable<OptionSource> result = CommandLine.Parse<TestType>(new[] {"--flag-"});
			Assert.Equal(result.Count(), 1);
			Assert.Equal(result.First().Name, "flag");
			Assert.False(result.First().IsTyped);
			Assert.Equal("-", result.First().Value);
		}

		[Fact]
		public void with_a_trailing_positive_should_return_the_symbol() {
			IEnumerable<OptionSource> result = CommandLine.Parse<TestType>(new[] {"--flag+"});
			Assert.Equal(result.Count(), 1);
			Assert.Equal(result.First().Name, "flag");
			Assert.False(result.First().IsTyped);
			Assert.Equal("+", result.First().Value);
		}

		[Fact]
		public void with_no_trailing_symbol_should_not_return_empty_string() {
			IEnumerable<OptionSource> result = CommandLine.Parse<TestType>(new[] {"--flag"});
			Assert.Equal(result.Count(), 1);
			Assert.Equal(result.First().Name, "flag");
			Assert.False(result.First().IsTyped);
			Assert.Equal("", result.First().Value);
		}
	}
}
