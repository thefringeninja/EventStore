using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Rags.Tests.EnvironmentTests {
	public class when_environment_variable_is_parsed : IDisposable {
		[Fact]
		public void should_return_the_environment_variables() {
			Environment.SetEnvironmentVariable("EVENTSTORE_NAME", "foo", EnvironmentVariableTarget.Process);
			var envVariable = Environment.GetEnvironmentVariable("EVENTSTORE_NAME");
			var result =
				EnvironmentVariables.Parse<TestType>(x => NameTranslators.PrefixEnvironmentVariable(x, "EVENTSTORE_"));
			var optionSource = Assert.Single(result);
			Assert.Equal("Name", optionSource.Name);
			Assert.False(optionSource.IsTyped);
			Assert.Equal("foo", optionSource.Value.ToString());
		}

		public void Dispose() {
			Environment.SetEnvironmentVariable("EVENTSTORE_NAME", null, EnvironmentVariableTarget.Process);
		}
	}
}
