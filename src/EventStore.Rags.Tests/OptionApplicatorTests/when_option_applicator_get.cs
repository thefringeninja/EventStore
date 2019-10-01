using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Rags.Tests.OptionApplicatorTests {
	public class when_option_applicator_get {
		[Fact]
		public void with_a_typed_option_that_exists_should_set() {
			var result = OptionApplicator.Get<TestType>(new[] {
				new OptionSource("test", "Flag", true, true)
			});
			Assert.True(result.Flag);
		}

		[Fact]
		public void with_a_non_typed_option_that_exists_should_set() {
			var result = OptionApplicator.Get<TestType>(new[] {
				new OptionSource("test", "Flag", false, "foo")
			});
			Assert.Equal("foo", result.Name);
		}

		[Fact]
		public void with_a_non_typed_complex_option_that_exists_should_set() {
			var result = OptionApplicator.Get<TestType>(new[] {
				new OptionSource("test", "IpEndpoint", false, "10.0.0.1:2113")
			});
			Assert.Equal(new IPEndPoint(IPAddress.Parse("10.0.0.1"), 2113), result.IpEndpoint);
		}

		[Fact]
		public void with_a_non_typed_option_that_does_not_exist_should_not_set() {
			var referenceType = new TestType();
			var result = OptionApplicator.Get<TestType>(new[] {
				new OptionSource("test", "NonExistent", false, "bar")
			});
			Assert.Equal(referenceType.Flag, result.Flag);
			Assert.Equal(referenceType.IpEndpoint, result.IpEndpoint);
			Assert.Equal(referenceType.Name, result.Name);
		}

		[Fact]
		public void with_a_non_typed_option_that_is_an_array_should_set() {
			var result = OptionApplicator.Get<TestType>(new[] {
				new OptionSource("test", "Names", false, new[] {"four", "five", "six"})
			});
			Assert.Equal(new[] {"four", "five", "six"}, result.Names);
		}

		[Fact]
		public void with_a_non_typed_complex_option_that_is_an_array_should_set() {
			var result = OptionApplicator.Get<TestType>(new[] {
				new OptionSource("test", "IpEndpoints", false, new[] {"10.0.0.1:2112", "10.0.0.2:2113"})
			});
			Assert.Equal(2, result.IpEndpoints.Length);
			Assert.Equal(new[] {
					new IPEndPoint(IPAddress.Parse("10.0.0.1"), 2112),
					new IPEndPoint(IPAddress.Parse("10.0.0.2"), 2113)
				},
				result.IpEndpoints);
		}
	}
}
