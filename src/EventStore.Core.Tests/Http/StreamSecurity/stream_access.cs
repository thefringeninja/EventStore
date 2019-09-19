using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using EventStore.ClientAPI;
using EventStore.Common.Utils;
using EventStore.Core.Services;
using EventStore.Core.Tests.Http.Users;
using NUnit.Framework;
using Newtonsoft.Json.Linq;

namespace EventStore.Core.Tests.Http.StreamSecurity {
	namespace stream_access {
		[TestFixture, Category("LongRunning")]
		class when_creating_a_secured_stream_by_posting_metadata : SpecificationWithUsers {
			private HttpResponseMessage _response;

			protected override void When() {
				var metadata =
					(StreamMetadata)
					StreamMetadata.Build()
						.SetMetadataReadRole("admin")
						.SetMetadataWriteRole("admin")
						.SetReadRole("")
						.SetWriteRole("other");
				var jsonMetadata = metadata.AsJsonString();
				_response = MakeArrayEventsPost(
					TestMetadataStream,
					new[] {
						new {
							EventId = Guid.NewGuid(),
							EventType = SystemEventTypes.StreamMetadata,
							Data = new JRaw(jsonMetadata)
						}
					}, _admin);
			}

			[Test]
			public void returns_ok_status_code() {
				Assert.AreEqual(HttpStatusCode.Created, _response.StatusCode);
			}

			[Test]
			public void refuses_to_post_event_as_anonymous() {
				var response = PostEvent(new {Some = "Data"});
				Assert.AreEqual(HttpStatusCode.Unauthorized, response.StatusCode);
			}

			[Test]
			public void accepts_post_event_as_authorized_user() {
				var response = PostEvent(new {Some = "Data"}, GetCorrectCredentialsFor("user1"));
				Assert.AreEqual(HttpStatusCode.Created, response.StatusCode);
			}

			[Test]
			public void accepts_post_event_as_authorized_user_by_trusted_auth() {
				var uri = MakeUrl(TestStream);

				var request = new HttpRequestMessage(HttpMethod.Post, uri) {
					Headers = {{"ES-TrustedAuth", "root; admin, other"}},
					Content = new ByteArrayContent(
						new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {Some = "Data"}}}
							.ToJsonBytes()) {
						Headers = {
							ContentType = MediaTypeHeaderValue.Parse("application/vnd.eventstore.events+json")
						}
					}
				};
				var response = GetRequestResponse(request);
				Assert.AreEqual(HttpStatusCode.Created, response.StatusCode);
			}
		}
	}
}
