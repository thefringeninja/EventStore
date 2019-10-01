using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace EventStore.Projections.Core.Tests.Other {
	public class when_matching_remaining_path {
		private UriTemplate _urlTemplate;
		private UriTemplateMatch _match;

		public when_matching_remaining_path() {
			_urlTemplate = new UriTemplate("/a/b/{*C}");
			_match = _urlTemplate.Match(new Uri("http://localhost"), new Uri("http://localhost/a/b/123"));
		}

		[Fact]
		public void bound_variable_c_is_available() {
			Assert.True(_match.BoundVariables.AllKeys.Contains("C"));
		}

		[Fact]
		public void bound_variable_c_contains_remaining_path() {
			Assert.Equal("123", _match.BoundVariables["C"]);
		}
	}

	public class when_matching_remaining_multi_segment_path {
		private UriTemplate _urlTemplate;
		private UriTemplateMatch _match;

		public when_matching_remaining_multi_segment_path() {
			_urlTemplate = new UriTemplate("/a/b/{*C}");
			_match = _urlTemplate.Match(new Uri("http://localhost"), new Uri("http://localhost/a/b/123/456"));
		}

		[Fact]
		public void bound_variable_c_is_available() {
			Assert.True(_match.BoundVariables.AllKeys.Contains("C"));
		}

		[Fact]
		public void bound_variable_c_contains_remaining_path() {
			Assert.Equal("123/456", _match.BoundVariables["C"]);
		}
	}


	public class when_matching_uri_with_missing_query_variable {
		private UriTemplate _urlTemplate;
		private UriTemplateMatch _match;

		public when_matching_uri_with_missing_query_variable() {
			_urlTemplate = new UriTemplate("/a/b?c={C}");
			_match = _urlTemplate.Match(new Uri("http://localhost"), new Uri("http://localhost/a/b"));
		}

		[Fact]
		public void match_succeeds() {
			Assert.True(_match != null);
		}

		[Fact]
		public void bound_variable_c_is_null() {
			Assert.Null(_match.BoundVariables["C"]);
		}
	}

	public class url_segments {
		[Fact]
		public void are_not_untumatically_unescaped() {
			var uri = new Uri("http://fake/a%24a%20/123$");
			Assert.Equal(3, uri.Segments.Length);
			Assert.Equal("/", uri.Segments[0]);
			Assert.Equal("a%24a%20/", uri.Segments[1]);
			Assert.Equal("123$", uri.Segments[2]);
		}

		[Fact]
		public void are_not_automatically_unescaped2() {
			var ub = new UriBuilder();
			ub.Scheme = "http";
			ub.Host = "fake";
			ub.Path = "/a%24a%20/123$";
			var uri = ub.Uri;
			Assert.Equal(3, uri.Segments.Length);
			Assert.Equal("/", uri.Segments[0]);
			Assert.Equal("a%24a%20/", uri.Segments[1]);
			Assert.Equal("123$", uri.Segments[2]);
		}
	}

	public class when_matching_escaped_urls {
		[Fact]
		public void Dump() {
			var result = new List<Tuple<char, string>>();
			for (char i = (char)1; i <= 127; i++) {
				try {
					var unescaped = "/z" + i + "z/";
					var escaped = "/z" + Uri.HexEscape(i) + "z/";

					var unescapedTemplate = new UriTemplate(unescaped);
					var escapedTemplate = new UriTemplate(escaped);

					Func<string, UriTemplate, bool> m =
						(s, template) =>
							template.Match(new Uri("http://localhost"), new Uri("http://localhost" + s)) != null;

					result.Add(Tuple.Create(i, string.Format(
						"e=>e {1}  e=>u {2}  u=>e {3} u=>u {4}", new String(i, 1), m(escaped, escapedTemplate),
						m(unescaped, escapedTemplate), m(escaped, unescapedTemplate),
						m(unescaped, unescapedTemplate))));
				} catch (Exception) {
					result.Add(
						Tuple.Create(i, string.Format("EXCEPTION")));
				}
			}

			foreach (var tuple in
				from i in result
				group i by i.Item2
				into g
				orderby g.Key
				select g
			) {
				Console.WriteLine(tuple.Key);
				Console.Write("   ");
				foreach (var i in tuple) {
					if (char.IsWhiteSpace(i.Item1))
						Console.Write(Uri.HexEscape(i.Item1));
					else
						Console.Write(i.Item1);
				}

				Console.WriteLine();
			}
		}

		private static void Matches(string template, string candidate) {
			var urlTemplate = new UriTemplate(template);
			var match = urlTemplate.Match(new Uri("http://localhost"), new Uri("http://localhost" + candidate));
			Assert.NotNull(match);
		}

		private static void DoesNotMatch(string template, string candidate) {
			var urlTemplate = new UriTemplate(template);
			var match = urlTemplate.Match(new Uri("http://localhost"), new Uri("http://localhost" + candidate));
			Assert.Null(match);
		}
	}
}
