using Xunit;
using System.Linq;

namespace EventStore.Rags.Tests.ExtensionsTests.NormalizeTests {
	public class when_normalizing {
		[Fact]
		public void with_a_positive_as_a_value_it_should_return_true_as_the_value() {
			var normalizedValues = new[] {new OptionSource("test", "flag", false, "+")}.Normalize();
			Assert.Single(normalizedValues);
			Assert.Equal("flag", normalizedValues.First().Name);
			Assert.True(normalizedValues.First().IsTyped);
			Assert.Equal(true, normalizedValues.First().Value);
		}

		[Fact]
		public void with_a_negative_as_a_value_it_should_return_false_as_the_value() {
			var normalizedValues = new[] {new OptionSource("test", "flag", false, "-")}.Normalize();
			Assert.Single(normalizedValues);
			Assert.Equal("flag", normalizedValues.First().Name);
			Assert.True(normalizedValues.First().IsTyped);
			Assert.Equal(false, normalizedValues.First().Value);
		}

		[Fact]
		public void with_an_empty_string_as_a_value_it_should_return_true_as_the_value() {
			var normalizedValues = new[] {new OptionSource("test", "flag", false, "")}.Normalize();
			Assert.Single(normalizedValues);
			Assert.Equal("flag", normalizedValues.First().Name);
			Assert.True(normalizedValues.First().IsTyped);
			Assert.Equal(true, normalizedValues.First().Value);
		}
	}
}
