using System;
using System.Text;
using System.Net.Http;
using System.Net.Http.Headers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;
using Newtonsoft.Json.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Core.Tests.Http.Users.users;

namespace EventStore.Core.Tests.Http.Streams {
	namespace idempotency {
		[SetUpFixture]
		abstract class HttpBehaviorSpecificationOfSuccessfulCreateEvent : with_admin_user {
			protected HttpResponseMessage _response;

			[OneTimeSetUp]
			public override void TestFixtureSetUp() {
				base.TestFixtureSetUp();
			}

			[OneTimeTearDown]
			public override void TestFixtureTearDown() {
				if (_response != null) {
					_response.Dispose();
				}

				base.TestFixtureTearDown();
			}

			[Test]
			public void response_should_not_be_null() {
				Assert.IsNotNull(_response);
			}

			[Test]
			public void returns_created_status_code() {
				Assert.AreEqual(HttpStatusCode.Created, _response.StatusCode);
			}

			[Test]
			public void returns_a_location_header() {
				Assert.IsNotEmpty(_response.Headers.GetLocationAsString());
			}

			[Test]
			public void returns_a_location_header_ending_with_zero() {
				var location = _response.Headers.GetLocationAsString();
				var tail = location.Substring(location.Length - "/0".Length);
				Assert.AreEqual("/0", tail);
			}

			[Test]
			public void returns_a_location_header_that_can_be_read_as_json() {
				var json = GetJson<JObject>(_response.Headers.GetLocationAsString());
				HelperExtensions.AssertJson(new {A = "1"}, json);
			}
		}

		[TestFixture]
		class when_posting_to_idempotent_guid_id_then_as_array : HttpBehaviorSpecificationOfSuccessfulCreateEvent {
			private Guid _eventId;

			protected override void Given() {
				_eventId = Guid.NewGuid();
				PostEvent();
			}

			protected override void When() {
				_response = MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = _eventId, EventType = "event-type", Data = new {A = "1"}}});
			}

			private void PostEvent() {
				var request = CreateRequest(TestStream + "/incoming/" + _eventId.ToString(), "", "POST",
					"application/json");
				request.Headers.Add("ES-EventType", "SomeType");
				var data = "{a : \"1\"}";
				var bytes = Encoding.UTF8.GetBytes(data);
				request.Content = new ByteArrayContent(bytes) {
					Headers = { ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = GetRequestResponse(request);
				Assert.AreEqual(HttpStatusCode.Created, _response.StatusCode);
			}
		}

		[TestFixture]
		class when_posting_to_idempotent_guid_id_twice : HttpBehaviorSpecificationOfSuccessfulCreateEvent {
			private Guid _eventId;

			protected override void Given() {
				_eventId = Guid.NewGuid();
				PostEvent();
			}

			protected override void When() {
				PostEvent();
			}

			private void PostEvent() {
				var request = CreateRequest(TestStream + "/incoming/" + _eventId.ToString(), "", "POST",
					"application/json");
				request.Headers.Add("ES-EventType", "SomeType");
				var data = "{a : \"1\"}";
				var bytes = Encoding.UTF8.GetBytes(data);
				request.Content = new ByteArrayContent(bytes) {
					Headers = { ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = GetRequestResponse(request);
				Assert.AreEqual(HttpStatusCode.Created, _response.StatusCode);
			}
		}


		[TestFixture]
		class when_posting_to_idempotent_guid_id_three_times : HttpBehaviorSpecificationOfSuccessfulCreateEvent {
			private Guid _eventId;

			protected override void Given() {
				_eventId = Guid.NewGuid();
				PostEvent();
				PostEvent();
			}

			protected override void When() {
				PostEvent();
			}

			private void PostEvent() {
				var request = CreateRequest(TestStream + "/incoming/" + _eventId.ToString(), "", "POST",
					"application/json");
				request.Headers.Add("ES-EventType", "SomeType");
				var data = "{a : \"1\"}";
				var bytes = Encoding.UTF8.GetBytes(data);
				request.Content = new ByteArrayContent(bytes) {
					Headers = { ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = GetRequestResponse(request);
				Assert.AreEqual(HttpStatusCode.Created, _response.StatusCode);
			}
		}


		[TestFixture, Category("LongRunning")]
		class when_posting_an_event_once_raw_once_with_array : HttpBehaviorSpecificationOfSuccessfulCreateEvent {
			private Guid _eventId;

			protected override void Given() {
				_eventId = Guid.NewGuid();
				PostEvent();
			}

			protected override void When() {
				_response = MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = _eventId, EventType = "event-type", Data = new {A = "1"}}});
			}

			private void PostEvent() {
				var request = CreateRequest(TestStream, "", "POST", "application/json");
				request.Headers.Add("ES-EventId", _eventId.ToString());
				request.Headers.Add("ES-EventType", "SomeType");
				var data = "{a : \"1\"}";
				var bytes = Encoding.UTF8.GetBytes(data);
				request.Content = new ByteArrayContent(bytes) {
					Headers = {ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = GetRequestResponse(request);
				Assert.AreEqual(HttpStatusCode.Created, _response.StatusCode);
			}
		}


		[TestFixture, Category("LongRunning")]
		class when_posting_an_event_twice_raw : HttpBehaviorSpecificationOfSuccessfulCreateEvent {
			private Guid _eventId;

			protected override void Given() {
				_eventId = Guid.NewGuid();
				PostEvent();
			}

			protected override void When() {
				PostEvent();
			}

			private void PostEvent() {
				var request = CreateRequest(TestStream, "", "POST", "application/json");
				request.Headers.Add("ES-EventId", _eventId.ToString());
				request.Headers.Add("ES-EventType", "SomeType");
				var data = "{a : \"1\"}";
				var bytes = Encoding.UTF8.GetBytes(data);
				request.Content = new ByteArrayContent(bytes) {
					Headers = { ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = GetRequestResponse(request);
				Assert.AreEqual(HttpStatusCode.Created, _response.StatusCode);
			}
		}

		[TestFixture, Category("LongRunning")]
		class when_posting_an_event_three_times_raw : HttpBehaviorSpecificationOfSuccessfulCreateEvent {
			private Guid _eventId;

			protected override void Given() {
				_eventId = Guid.NewGuid();
				PostEvent();
				PostEvent();
			}

			protected override void When() {
				PostEvent();
			}

			private void PostEvent() {
				var request = CreateRequest(TestStream, "", "POST", "application/json");
				request.Headers.Add("ES-EventId", _eventId.ToString());
				request.Headers.Add("ES-EventType", "SomeType");
				var data = "{a : \"1\"}";
				var bytes = Encoding.UTF8.GetBytes(data);
				request.Content = new ByteArrayContent(bytes) {
					Headers = { ContentType = new MediaTypeHeaderValue("application/json")}
				};
				_response = GetRequestResponse(request);
				Assert.AreEqual(HttpStatusCode.Created, _response.StatusCode);
			}
		}

		[TestFixture, Category("LongRunning")]
		class when_posting_an_event_twice_array : HttpBehaviorSpecificationOfSuccessfulCreateEvent {
			private Guid _eventId;

			protected override void Given() {
				_eventId = Guid.NewGuid();
				var response1 = MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = _eventId, EventType = "event-type", Data = new {A = "1"}}});
				Assert.AreEqual(HttpStatusCode.Created, response1.StatusCode);
			}

			protected override void When() {
				_response = MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = _eventId, EventType = "event-type", Data = new {A = "1"}}});
			}
		}


		[TestFixture, Category("LongRunning")]
		class when_posting_an_event_three_times_as_array : HttpBehaviorSpecificationOfSuccessfulCreateEvent {
			private Guid _eventId;

			protected override void Given() {
				_eventId = Guid.NewGuid();
				var response1 = MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = _eventId, EventType = "event-type", Data = new {A = "1"}}});
				Assert.AreEqual(HttpStatusCode.Created, response1.StatusCode);
				var response2 = MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = _eventId, EventType = "event-type", Data = new {A = "1"}}});
				Assert.AreEqual(HttpStatusCode.Created, response2.StatusCode);
			}

			protected override void When() {
				_response = MakeArrayEventsPost(
					TestStream,
					new[] {new {EventId = _eventId, EventType = "event-type", Data = new {A = "1"}}});
			}
		}
	}
}
