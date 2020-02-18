using System;
using System.Collections.Generic;
using System.Net.Http.Headers;
using EventStore.Core.Services.Transport.Http;
using EventStore.Transport.Http.Codecs;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace EventStore.Transport.Http {
	public class ResponseCodecTests {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {
				new[] {new MediaTypeWithQualityHeaderValue("application/json", 1.0)},
				new ICodec[] {Codec.Text, Codec.Json, Codec.Xml, Codec.CompetingJson},
				Codec.Json
			};

			yield return new object[] {
				new[] {
					new MediaTypeWithQualityHeaderValue("application/*", 1.0),
					new MediaTypeWithQualityHeaderValue("text/xml", 0.9)
				},
				new ICodec[] {Codec.Text, Codec.Json, Codec.Xml, Codec.CompetingJson},
				Codec.Json
			};

			yield return new object[] {
				new[] {
					new MediaTypeWithQualityHeaderValue("application/json", 0.9),
					new MediaTypeWithQualityHeaderValue("text/xml", 1.0)
				},
				new ICodec[] {Codec.Text, Codec.Json, Codec.Xml, Codec.CompetingJson},
				Codec.Xml
			};

			yield return new object[] {
				new[] {
					new MediaTypeWithQualityHeaderValue("*/*", 1.0),
					new MediaTypeWithQualityHeaderValue("application/json", 0.9),
					new MediaTypeWithQualityHeaderValue("text/xml", 1.0)
				},
				new ICodec[] {Codec.Text, Codec.Json, Codec.Xml, Codec.CompetingJson},
				Codec.Text
			};

			yield return new object[] {
				new[] {
					new MediaTypeWithQualityHeaderValue("*/*", 1.0),
					new MediaTypeWithQualityHeaderValue("application/json", 0.9),
					new MediaTypeWithQualityHeaderValue("text/xml", 1.0)
				},
				Array.Empty<ICodec>(),
				null
			};

			yield return new object[] {
				Array.Empty<MediaTypeWithQualityHeaderValue>(),
				new ICodec[] {Codec.Text, Codec.Json, Codec.Xml, Codec.CompetingJson},
				Codec.Text
			};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void Select(MediaTypeWithQualityHeaderValue[] accept, ICodec[] acceptable, ICodec expected) {
			var sut = new ResponseCodecs(acceptable);

			var actual = sut.Select(new DefaultHttpContext {
				Request = {
					Headers = {
						{"accept", new StringValues(Array.ConvertAll(accept, x => x.ToString()))}
					}
				}
			}.Request);

			Assert.Same(expected, actual);
		}
	}
}
