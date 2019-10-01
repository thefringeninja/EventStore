using EventStore.Common.Utils;
using EventStore.Transport.Http;
using Xunit;

namespace EventStore.Core.Tests.Services.Transport.Http {
	public class media_type {
		[Fact]
		public void parses_generic_wildcard() {
			var c = MediaType.Parse("*/*");

			Assert.Equal("*", c.Type);
			Assert.Equal("*", c.Subtype);
			Assert.Equal("*/*", c.Range);
			Assert.Equal(1.0f, c.Priority);
			Assert.False(c.EncodingSpecified);
			Assert.Null(c.Encoding);
		}

		[Fact]
		public void parses_generic_wildcard_with_priority() {
			var c = MediaType.Parse("*/*;q=0.4");

			Assert.Equal("*", c.Type);
			Assert.Equal("*", c.Subtype);
			Assert.Equal("*/*", c.Range);
			Assert.Equal(0.4f, c.Priority);
			Assert.False(c.EncodingSpecified);
			Assert.Null(c.Encoding);
		}

		[Fact]
		public void parses_generic_wildcard_with_priority_and_other_param() {
			var c = MediaType.Parse("*/*;q=0.4;z=7");

			Assert.Equal("*", c.Type);
			Assert.Equal("*", c.Subtype);
			Assert.Equal("*/*", c.Range);
			Assert.Equal(0.4f, c.Priority);
			Assert.False(c.EncodingSpecified);
			Assert.Null(c.Encoding);
		}

		[Fact]
		public void parses_partial_wildcard() {
			var c = MediaType.Parse("text/*");

			Assert.Equal("text", c.Type);
			Assert.Equal("*", c.Subtype);
			Assert.Equal("text/*", c.Range);
			Assert.Equal(1f, c.Priority);
			Assert.False(c.EncodingSpecified);
			Assert.Null(c.Encoding);
		}

		[Fact]
		public void parses_specific_media_range_with_priority() {
			var c = MediaType.Parse("application/xml;q=0.7");

			Assert.Equal("application", c.Type);
			Assert.Equal("xml", c.Subtype);
			Assert.Equal("application/xml", c.Range);
			Assert.Equal(0.7f, c.Priority);
			Assert.False(c.EncodingSpecified);
			Assert.Null(c.Encoding);
		}

		[Fact]
		public void parses_with_encoding() {
			var c = MediaType.Parse("application/json;charset=utf-8");

			Assert.Equal("application", c.Type);
			Assert.Equal("json", c.Subtype);
			Assert.Equal("application/json", c.Range);
			Assert.True(c.EncodingSpecified);
			Assert.Equal(Helper.UTF8NoBom, c.Encoding);
		}

		[Fact]
		public void parses_with_encoding_and_priority() {
			var c = MediaType.Parse("application/json;q=0.3;charset=utf-8");

			Assert.Equal("application", c.Type);
			Assert.Equal("json", c.Subtype);
			Assert.Equal("application/json", c.Range);
			Assert.Equal(0.3f, c.Priority);
			Assert.True(c.EncodingSpecified);
			Assert.Equal(Helper.UTF8NoBom, c.Encoding);
		}

		[Fact]
		public void parses_unknown_encoding() {
			var c = MediaType.Parse("application/json;charset=woftam");

			Assert.Equal("application", c.Type);
			Assert.Equal("json", c.Subtype);
			Assert.Equal("application/json", c.Range);
			Assert.True(c.EncodingSpecified);
			Assert.Null(c.Encoding);
		}

		[Fact]
		public void parses_with_spaces_between_parameters() {
			var c = MediaType.Parse("application/json; charset=woftam");

			Assert.Equal("application", c.Type);
			Assert.Equal("json", c.Subtype);
			Assert.Equal("application/json", c.Range);
			Assert.True(c.EncodingSpecified);
			Assert.Null(c.Encoding);
		}

		[Fact]
		public void parses_upper_case_parameters() {
			var c = MediaType.Parse("application/json; charset=UTF-8");

			Assert.Equal("application", c.Type);
			Assert.Equal("json", c.Subtype);
			Assert.Equal("application/json", c.Range);
			Assert.True(c.EncodingSpecified);
			Assert.Equal(Helper.UTF8NoBom, c.Encoding);
		}
	}
}
