using Xunit;
using System.Linq;

namespace EventStore.Rags.Tests.ExtensionsTests.UseAliasTests {
	public class when_using_aliases {
		public class TestType {
			[ArgAlias("FirstFlagAlias")] public string FirstFlag { get; set; }

			[ArgAlias("SecondFlagAlias", "AlternateSecondFlagAlias")]
			public string SecondFlag { get; set; }
		}

		[Fact]
		public void with_a_property_that_contains_a_single_alias() {
			var optionSources = new OptionSource[] {new OptionSource("test", "firstflagalias", false, "value")}
				.UseAliases<TestType>();
			Assert.Equal(1, optionSources.Count());
			Assert.NotNull(optionSources.FirstOrDefault(x =>
				x.Name.Equals("firstflag", System.StringComparison.OrdinalIgnoreCase)));
			Assert.Equal("value",
				optionSources.FirstOrDefault(x => x.Name.Equals("firstflag", System.StringComparison.OrdinalIgnoreCase))
					.Value);
		}

		[Fact]
		public void with_a_property_that_contains_a_single_alias_but_not_used() {
			var optionSources = new OptionSource[] {new OptionSource("test", "firstflag", false, "value")}
				.UseAliases<TestType>();
			Assert.Equal(1, optionSources.Count());
			Assert.NotNull(optionSources.FirstOrDefault(x =>
				x.Name.Equals("firstflag", System.StringComparison.OrdinalIgnoreCase)));
			Assert.Equal("value",
				optionSources.FirstOrDefault(x => x.Name.Equals("firstflag", System.StringComparison.OrdinalIgnoreCase))
					.Value);
		}

		[Fact]
		public void with_a_property_that_contains_multiple_aliases() {
			var optionSources = new OptionSource[]
				{new OptionSource("test", "alternatesecondflagalias", false, "value")}.UseAliases<TestType>();
			Assert.Equal(1, optionSources.Count());
			Assert.NotNull(optionSources.FirstOrDefault(x =>
				x.Name.Equals("secondflag", System.StringComparison.OrdinalIgnoreCase)));
			Assert.Equal("value",
				optionSources
					.FirstOrDefault(x => x.Name.Equals("secondflag", System.StringComparison.OrdinalIgnoreCase)).Value);
		}
	}
}
