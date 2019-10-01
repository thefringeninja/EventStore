using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Rags.Tests.CommandLineTests {
	public class when_no_arguments_is_parsed {
		[Fact]
		public void it_should_return_no_results() {
			IEnumerable<OptionSource> result = CommandLine.Parse<TestType>(null);
			Assert.Equal(result.Count(), 0);
		}
	}
}
