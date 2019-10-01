using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Rags.Tests.EnvironmentTests {
	public class when_referenced_environment_variable_is_parsed : IDisposable {
		[Fact]
		public void should_return_the_referenced_environment_variable() {
			Environment.SetEnvironmentVariable("EVENTSTORE_NAME", "${env:TEST_REFERENCE_VAR}",
				EnvironmentVariableTarget.Process);
			Environment.SetEnvironmentVariable("TEST_REFERENCE_VAR", "foo", EnvironmentVariableTarget.Process);

			var envVariable = Environment.GetEnvironmentVariable("EVENTSTORE_NAME");
			var result =
				EnvironmentVariables.Parse<TestType>(x => NameTranslators.PrefixEnvironmentVariable(x, "EVENTSTORE_"));
			var optionSource = Assert.Single(result);
			Assert.Equal("Name", optionSource.Name);
			Assert.False(optionSource.IsTyped);
			Assert.Equal("foo", optionSource.Value.ToString());
			Assert.True(optionSource.IsReference);
		}

		[Fact]
		public void should_return_null_if_referenced_environment_variable_does_not_exist() {
			Environment.SetEnvironmentVariable("EVENTSTORE_NAME", "${env:TEST_REFERENCE_VAR}",
				EnvironmentVariableTarget.Process);
			var envVariable = Environment.GetEnvironmentVariable("EVENTSTORE_NAME");
			var result =
				EnvironmentVariables.Parse<TestType>(x => NameTranslators.PrefixEnvironmentVariable(x, "EVENTSTORE_"));
			var optionSource = Assert.Single(result);
			Assert.Equal("Name", optionSource.Name);
			Assert.False(optionSource.IsTyped);
			Assert.Null(optionSource.Value);
			Assert.True(optionSource.IsReference);
		}

		public void Dispose() {
			Environment.SetEnvironmentVariable("EVENTSTORE_NAME", null, EnvironmentVariableTarget.Process);
			Environment.SetEnvironmentVariable("TEST_REFERENCE_VAR", null, EnvironmentVariableTarget.Process);
		}
	}
}
