using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using EventStore.Core.Services.Transport.Http;
using EventStore.Transport.Http.Codecs;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using static System.Net.HttpStatusCode;
using static System.Net.Http.HttpMethod;

namespace EventStore.Transport.Http {
	public class ContentNegotiationTests {
		public static IEnumerable<object[]> RequestNegotiationCases() {
			var hasBody = new[] {
				Post,
				Put,
				Delete,
				Patch
			};
			yield return new object[] {
				Get, null, Array.Empty<ICodec>(), OK
			};
			yield return new object[] {
				Get, null, Array.Empty<ICodec>(), OK
			};
			foreach (var method in hasBody) {
				yield return new object[] {
					method, new MediaTypeHeaderValue("application/json"),
					new ICodec[] {Codec.Json, Codec.Xml, Codec.Text}, OK
				};
				yield return new object[] {
					method, new MediaTypeHeaderValue("text/plain"),
					new ICodec[] {Codec.Json}, UnsupportedMediaType
				};
			}
		}

		[Theory, MemberData(nameof(RequestNegotiationCases))]
		public async Task request_negotiation(System.Net.Http.HttpMethod method, MediaTypeHeaderValue contentType,
			ICodec[] supported, System.Net.HttpStatusCode expected) {
			using var server = new TestServer(new WebHostBuilder()
				.ConfigureServices(services => services.AddRouting())
				.Configure(app => app
					.UseRouting()
					.UseContentNegotiation()
					.UseEndpoints(endpoints => endpoints
						.MapMethods("/", new[] {method.ToString()}, RequestDelegate)
						.WithMetadata(new RequestCodecs(supported), new ResponseCodecs(new[] {Codec.Json})))));
			using var client = server.CreateClient();

			using var response = await client.SendAsync(new HttpRequestMessage(method, "/") {
				Content = new ReadOnlyMemoryContent(Codec.Json.Encoding.GetBytes("{}")) {
					Headers = {ContentType = contentType}
				}
			});

			Assert.Equal(expected, response.StatusCode);

			if (expected == OK) {
				Assert.Equal(new MediaTypeHeaderValue(Codec.Json.ContentType), response.Content.Headers.ContentType);
			}
		}

		public static IEnumerable<object[]> ResponseNegotiationCases() {
			yield return new object[] {
				Array.Empty<MediaTypeWithQualityHeaderValue>(), new[] {Codec.Json}, OK,
				new MediaTypeHeaderValue(Codec.Json.ContentType),
			};
			yield return new object[] {
				new[] {new MediaTypeWithQualityHeaderValue(Codec.Json.ContentType)}, new[] {Codec.Json}, OK,
				new MediaTypeHeaderValue(Codec.Json.ContentType),
			};
			yield return new object[] {
				new[] {new MediaTypeWithQualityHeaderValue(Codec.Json.ContentType)}, new[] {Codec.Xml}, NotAcceptable,
				null,
			};
		}

		[Theory, MemberData(nameof(ResponseNegotiationCases))]
		public async Task response_negotiation(MediaTypeWithQualityHeaderValue[] accept, ICodec[] acceptable,
			System.Net.HttpStatusCode expectedStatusCode, MediaTypeHeaderValue expectedContentType) {
			using var server = new TestServer(new WebHostBuilder()
				.ConfigureServices(services => services.AddRouting())
				.Configure(app => app
					.UseRouting()
					.UseContentNegotiation()
					.UseEndpoints(endpoints => endpoints
						.MapGet("/", RequestDelegate)
						.WithMetadata(RequestCodecs.None, new ResponseCodecs(acceptable)))));
			using var client = server.CreateClient();

			using var response = await client.SendAsync(new HttpRequestMessage(Get, "/") {
				Headers = {
					{"accept", accept.Select(x => x.ToString())}
				}
			});

			Assert.Equal(expectedStatusCode, response.StatusCode);

			Assert.Equal(expectedContentType, response.Content.Headers.ContentType);
		}

		private static Task RequestDelegate(HttpContext context) {
			context.Response.StatusCode = 200;
			context.SetEventStoreResponse(new object());
			return Task.CompletedTask;
		}
	}
}
