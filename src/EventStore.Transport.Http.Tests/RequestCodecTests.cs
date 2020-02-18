using System.Collections.Generic;
using System.Net.Http.Headers;
using EventStore.Core.Services.Transport.Http;
using EventStore.Transport.Http.Codecs;
using Microsoft.AspNetCore.Http;
using Xunit;

namespace EventStore.Transport.Http {
	public class RequestCodecTests {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {
				new MediaTypeHeaderValue("application/json"),
				new ICodec[] {Codec.Text, Codec.Json, Codec.Xml, Codec.CompetingJson},
				Codec.Json
			};

			yield return new object[] {
				null,
				new ICodec[] {Codec.Text, Codec.Json, Codec.Xml, Codec.CompetingJson},
				null
			};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void Select(MediaTypeHeaderValue contentType, ICodec[] supported, ICodec expected) {
			var sut = new RequestCodecs(supported);

			var actual = sut.Select(new DefaultHttpContext {
				Request = {
					ContentType = contentType?.ToString()
				}
			}.Request);

			Assert.Same(expected, actual);
		}
	}
}
