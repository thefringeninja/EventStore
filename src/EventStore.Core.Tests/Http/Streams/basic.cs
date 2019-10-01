using System;
using System.Text;
using EventStore.Core.Tests.Helpers;
using EventStore.Transport.Http;
using Xunit;
using Newtonsoft.Json.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;
using System.Linq;
using System.Xml.Linq;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using EventStore.Core.Tests.Http.Users.users;
using HttpMethod = EventStore.Transport.Http.HttpMethod;

namespace EventStore.Core.Tests.Http.Streams {
	namespace basic {
		[Trait("Category", "LongRunning")]
		public class
			when_requesting_a_single_event_in_the_stream_as_atom_json : HttpBehaviorSpecificationWithSingleEvent {
			private JObject _json;

			protected override async Task When() {
				_json = await GetJson<JObject>(TestStream + "/0", accept: ContentType.AtomJson);
			}

			[Fact]
			public void request_succeeds() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}

			[Fact]
			public void returns_correct_body() {
				HelperExtensions.AssertJson(new {Content = new {Data = new {A = "1"}}}, _json);
			}
		}

		[Trait("Category", "LongRunning")]
		public class
			when_requesting_a_single_event_in_the_stream_as_atom_xml : HttpBehaviorSpecificationWithSingleEvent {
			private XDocument document;

			protected override async Task When() {
				await Get(TestStream + "/0", "", accept: ContentType.Atom);
				document = XDocument.Parse(_lastResponseBody);
			}

			[Fact]
			public void request_succeeds() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}

			[Fact]
			public void returns_correct_body() {
				var val = document.GetEntry()
					.Elements(XDocumentAtomExtensions.AtomNamespace + "content").First()
					.Element("data")
					.Element("a").Value;
				Assert.Equal(val, "1");
			}
		}

		public class when_posting_an_event_as_raw_json_without_eventtype : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeJsonPost(
					TestStream,
					new {A = "1", B = "3", C = "5"});
			}

			[Fact]
			public void returns_bad_request_status_code() {
				Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
			}
		}

		public class when_posting_an_event_to_idempotent_uri_as_events_array : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeArrayEventsPost(
					TestStream + "/incoming/" + Guid.NewGuid().ToString(),
					new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
			}

			[Fact]
			public void returns_bad_request_status_code() {
				Assert.Equal(HttpStatusCode.UnsupportedMediaType, _response.StatusCode);
			}
		}

		public class when_posting_an_event_as_json_to_idempotent_uri_without_event_type : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeJsonPost(
					TestStream + "/incoming/" + Guid.NewGuid().ToString(),
					new {A = "1", B = "3", C = "5"});
			}

			[Fact]
			public void returns_bad_request_status_code() {
				Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
			}
		}


		public class when_posting_an_event_in_json_to_idempotent_uri_without_event_id : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest(TestStream + "/incoming/" + Guid.NewGuid(), "", "POST",
					"application/json");
				request.Headers.Add("ES-EventType", "SomeType");
				var data = "{a : \"1\", b:\"3\", c:\"5\" }";
				var bytes = Encoding.UTF8.GetBytes(data);
				request.Content = new ByteArrayContent(bytes) {
					Headers = {ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = await GetRequestResponse(request);
			}

			[Fact]
			public void returns_created_status_code() {
				Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.True(!string.IsNullOrEmpty(_response.Headers.GetLocationAsString()));
			}

			[Fact]
			public async Task returns_a_location_header_that_can_be_read_as_json() {
				var json = await GetJson<JObject>(_response.Headers.GetLocationAsString());
				HelperExtensions.AssertJson(new {a = "1", b = "3", c = "5"}, json);
			}
		}

		public class when_posting_an_event_as_raw_json_without_eventid : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest(TestStream, "", "POST", "application/json");
				request.Headers.Add("ES-EventType", "SomeType");
				var data = "{A : \"1\", B:\"3\", C:\"5\" }";
				var bytes = Encoding.UTF8.GetBytes(data);
				request.Content = new ByteArrayContent(bytes) {
					Headers = {ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = await GetRequestResponse(request);
			}

			[Fact]
			public void returns_redirectkeepverb_status_code() {
				Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.True(!string.IsNullOrEmpty(_response.Headers.GetLocationAsString()));
			}

			[Fact]
			public void returns_a_to_incoming() {
				Assert.True(_response.Headers.GetLocationAsString().Contains("/incoming/"));
				//HelperExtensions.AssertJson(new {A = "1"}, json);
			}
		}

		public class when_posting_an_event_as_array_with_no_event_type : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = Guid.NewGuid(), Data = new {A = "1"}}});
			}

			[Fact]
			public void returns_bad_request_status_code() {
				Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
			}
		}


		public class when_posting_an_event_as_array : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
			}

			[Fact]
			public void returns_created_status_code() {
				Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public async Task returns_a_location_header_that_can_be_read_as_json() {
				var json = await GetJson<JObject>(_response.Headers.GetLocationAsString());
				HelperExtensions.AssertJson(new {A = "1"}, json);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_posting_an_event_as_array_to_stream_with_slash : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest(TestStream + "/", "", "POST", "application/vnd.eventstore.events+json",
					null);
				request.Content = new ByteArrayContent(new[]
					{new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}}.ToJsonBytes()) {
					Headers = { ContentType = new MediaTypeHeaderValue("application/vnd.eventstore.events+json")}
				};
				_response = await _client.SendAsync(request);
			}

			[Fact]
			public void returns_permanent_redirect() {
				Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
			}

			[Fact]
			public void redirect_is_cacheable() {
				Assert.Equal(CacheControlHeaderValue.Parse("max-age=31536000, public"),
					_response.Headers.CacheControl);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public void returns_a_location_header_that_is_to_stream_without_slash() {
				Assert.Equal(MakeUrl(TestStream).ToString(), _response.Headers.GetLocationAsString());
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_deleting_to_stream_with_slash : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest(TestStream + "/", "", "DELETE", "application/json", null);
				_response = await _client.SendAsync(request);
			}

			[Fact]
			public void returns_permanent_redirect() {
				Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public void returns_a_location_header_that_is_to_stream_without_slash() {
				Assert.Equal(MakeUrl(TestStream).ToString(), _response.Headers.GetLocationAsString());
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_getting_from_stream_with_slash : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest(TestStream + "/", "", "GET", "application/json", null);
				_response = await _client.SendAsync(request);
			}

			[Fact]
			public void returns_permanent_redirect() {
				Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public void returns_a_location_header_that_is_to_stream_without_slash() {
				Assert.Equal(MakeUrl(TestStream).ToString(), _response.Headers.GetLocationAsString());
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_getting_from_all_stream_with_slash : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest("/streams/$all/", "", "GET", "application/json", DefaultData.AdminNetworkCredentials);
				_response = await _client.SendAsync(request);
			}

			[Fact]
			public void returns_permanent_redirect() {
				Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public void returns_a_location_header_that_is_to_stream_without_slash() {
				Assert.Equal(MakeUrl("/streams/$all").ToString(), _response.Headers.GetLocationAsString());
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_getting_from_encoded_all_stream_with_slash : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest("/streams/$all/", "", "GET", "application/json", null);
				request.Headers.Authorization = new AuthenticationHeaderValue("Basic",
					GetAuthorizationHeader(DefaultData.AdminNetworkCredentials));
				_response = await _client.SendAsync(request);
			}

			[Fact]
			public void returns_permanent_redirect() {
				Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public void returns_a_location_header_that_is_to_stream_without_slash() {
				Assert.Equal(MakeUrl("/streams/$all").ToString(), _response.Headers.GetLocationAsString());
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_posting_an_event_as_array_to_metadata_stream_with_slash : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest(TestStream + "/metadata/", "", "POST", "application/json", null);
				request.Content = new ByteArrayContent(
					new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}}
						.ToJsonBytes()) {
					Headers = { ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = await _client.SendAsync(request);
			}

			[Fact]
			public void returns_permanent_redirect() {
				Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public void returns_a_location_header_that_is_to_stream_without_slash() {
				Assert.Equal(MakeUrl(TestStream + "/metadata").ToString(), _response.Headers.GetLocationAsString());
			}
		}


		[Trait("Category", "LongRunning")]
		public class when_getting_from_metadata_stream_with_slash : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				var request = CreateRequest(TestStream + "/metadata/", "", "GET", "application/json", null);
				request.Headers.Authorization = new AuthenticationHeaderValue("Basic",
					GetAuthorizationHeader(DefaultData.AdminNetworkCredentials));
				_response = await _client.SendAsync(request);
			}

			[Fact]
			public void returns_permanent_redirect() {
				Assert.Equal(HttpStatusCode.RedirectKeepVerb, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public void returns_a_location_header_that_is_to_stream_without_slash() {
				Assert.Equal(MakeUrl(TestStream + "/metadata").ToString(), _response.Headers.GetLocationAsString());
			}
		}


		[Trait("Category", "LongRunning")]
		public class when_posting_an_event_without_EventId_as_array : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeJsonPost(
					TestStream,
					new[] {new {EventType = "event-type", Data = new {A = "1"}}});
			}

			[Fact]
			public void returns_bad_request_status_code() {
				Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_posting_an_event_without_EventType_as_array : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = Guid.NewGuid(), Data = new {A = "1"}}});
			}

			[Fact]
			public void returns_bad_request_status_code() {
				Assert.Equal(HttpStatusCode.BadRequest, _response.StatusCode);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_posting_an_event_with_date_time : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeArrayEventsPost(
					TestStream,
					new[] {
						new {
							EventId = Guid.NewGuid(),
							EventType = "event-type",
							Data = new {A = "1987-11-07T00:00:00.000+01:00"}
						},
					});
			}

			[Fact]
			public void returns_created_status_code() {
				Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public async Task the_json_data_is_not_mangled() {
				var json = await GetJson<JObject>(_response.Headers.GetLocationAsString());
				HelperExtensions.AssertJson(new {A = "1987-11-07T00:00:00.000+01:00"}, json);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_posting_an_events_as_array : with_admin_user {
			private HttpResponseMessage _response;

			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				_response = await MakeArrayEventsPost(
					TestStream,
					new[] {
						new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}},
						new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "2"}},
					});
			}

			[Fact]
			public void returns_created_status_code() {
				Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
			}

			[Fact]
			public void returns_a_location_header() {
				Assert.NotEmpty(_response.Headers.GetLocationAsString());
			}

			[Fact]
			public async Task returns_a_location_header_for_the_first_posted_event() {
				var json = await GetJson<JObject>(_response.Headers.GetLocationAsString());
				HelperExtensions.AssertJson(new {A = "1"}, json);
			}
		}

		public abstract class HttpBehaviorSpecificationWithSingleEvent : with_admin_user {
			protected HttpResponseMessage _response;

			protected override async Task Given() {
				_response = await MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = Guid.NewGuid(), EventType = "event-type", Data = new {A = "1"}}});
				Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
			}
		}


		[Trait("Category", "LongRunning")]
		public class
			when_requesting_a_single_event_that_is_deleted_linkto : HttpSpecificationWithLinkToToDeletedEvents {
			protected override Task When() {
				return Get("/streams/" + LinkedStreamName + "/0", "", "application/json", credentials: DefaultData.AdminNetworkCredentials);
			}

			[Fact]
			public void the_event_is_gone() {
				Assert.Equal(HttpStatusCode.NotFound, _lastResponse.StatusCode);
			}
		}

		[Trait("Category", "LongRunning")]
		public class
			when_requesting_a_single_event_that_is_maxcount_deleted_linkto :
				SpecificationWithLinkToToMaxCountDeletedEvents {
			protected override Task When() {
				return Get("/streams/" + LinkedStreamName + "/0", "", "application/json", DefaultData.AdminNetworkCredentials);
			}

			[Fact]
			public void the_event_is_gone() {
				Assert.Equal(HttpStatusCode.NotFound, _lastResponse.StatusCode);
			}
		}


		[Trait("Category", "LongRunning")]
		public class
			when_requesting_a_single_event_in_the_stream_without_an_accept_header :
				HttpBehaviorSpecificationWithSingleEvent {
			private XDocument _xmlDocument;

			protected override async Task When() {
				_xmlDocument = await GetXml(MakeUrl(TestStream + "/0"));
			}

			[Fact]
			public void request_succeeds() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}

			[Fact]
			public void returns_correct_body() {
				Assert.Equal("1", _xmlDocument.Element("data").Element("a").Value);
			}
		}

		[Trait("Category", "LongRunning")]
		public class
			when_requesting_a_single_event_in_the_stream_as_event_json : HttpBehaviorSpecificationWithSingleEvent {
			private JObject _json;

			protected override async Task When() {
				_json = await GetJson<JObject>(TestStream + "/0", accept: ContentType.EventJson);
			}

			[Fact]
			public void request_succeeds() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}

			[Fact]
			public void returns_correct_body() {
				HelperExtensions.AssertJson(new {Data = new {A = "1"}}, _json);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_requesting_a_single_event_in_the_stream_as_json : HttpBehaviorSpecificationWithSingleEvent {
			private JObject _json;

			protected override async Task When() {
				_json = await GetJson<JObject>(TestStream + "/0", accept: ContentType.Json);
			}

			[Fact]
			public void request_succeeds() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}

			[Fact]
			public void returns_correct_body() {
				HelperExtensions.AssertJson(new {A = "1"}, _json);
			}
		}

		[Trait("Category", "LongRunning")]
		public class
			when_requesting_a_single_event_in_the_stream_as_event_xml : HttpBehaviorSpecificationWithSingleEvent {
			protected override Task When() {
				return Get(TestStream + "/0", "", accept: ContentType.EventXml);
			}

			[Fact]
			public void request_succeeds() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_requesting_a_single_event_in_the_stream_as_xml : HttpBehaviorSpecificationWithSingleEvent {
			protected override Task When() {
				return Get(TestStream + "/0", "", accept: ContentType.Xml);
			}

			[Fact]
			public void request_succeeds() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}
		}

		public class when_requesting_a_single_raw_event_in_the_stream_as_raw : with_admin_user {
			protected HttpResponseMessage _response;
			protected byte[] _data;

			protected override async Task Given() {
				var request = CreateRequest(TestStream, String.Empty, HttpMethod.Post, "application/octet-stream");
				request.Headers.Add("ES-EventType", "TestEventType");
				request.Headers.Add("ES-EventID", Guid.NewGuid().ToString());
				if (_data == null) {
					var fileData = HelperExtensions.GetFilePathFromAssembly("Resources/es-tile.png");
					_data = File.ReadAllBytes(fileData);
				}
				request.Content = new ByteArrayContent(_data) {
					Headers = {ContentType = new MediaTypeHeaderValue("application/octet-stream")}
				};
				_response = await GetRequestResponse(request);
				Assert.Equal(HttpStatusCode.Created, _response.StatusCode);
			}

			protected override Task When() {
				return Get(TestStream + "/0", "", "application/octet-stream");
			}

			[Fact]
			public void returns_correct_body() {
				Assert.Equal(_data, _lastResponseBytes);
			}
		}
	}
}
