using System;
using System.Net;
using System.Text;
using EventStore.Core.Tests.ClientAPI;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.Http.Users;
using EventStore.Transport.Http;
using Xunit;
using System.Linq;
using Newtonsoft.Json.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Core.Services.Transport.Http;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.Core.Tests.Http.Users.users;

namespace EventStore.Core.Tests.Http.Streams {

	[Trait("Category", "LongRunning")]
	public class when_getting_a_stream_without_accept_header : with_admin_user {
		private JObject _descriptionDocument;
		private List<JToken> _links;

		protected override Task Given() => Task.CompletedTask;

		protected override async Task When() {
			_descriptionDocument = await GetJsonWithoutAcceptHeader<JObject>(TestStream);
		}

		[Fact]
		public void returns_not_acceptable() {
			Assert.Equal(HttpStatusCode.NotAcceptable, _lastResponse.StatusCode);
		}

		[Fact]
		public void returns_a_description_document() {
			Assert.NotNull(_descriptionDocument);
			_links = _descriptionDocument != null ? _descriptionDocument["_links"].ToList() : new List<JToken>();
			Assert.NotNull(_links);
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_getting_a_stream_with_description_document_media_type : with_admin_user {
		private JObject _descriptionDocument;
		private List<JToken> _links;

		protected override Task Given() => Task.CompletedTask;

		protected override async Task When() {
			_descriptionDocument = await GetJson<JObject>(TestStream, "application/vnd.eventstore.streamdesc+json", null);
		}

		[Fact]
		public void returns_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void returns_a_description_document() {
			Assert.NotNull(_descriptionDocument);
			_links = _descriptionDocument != null ? _descriptionDocument["_links"].ToList() : new List<JToken>();
			Assert.NotNull(_links);
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_getting_description_document : with_admin_user {
		private JObject _descriptionDocument;
		private List<JToken> _links;

		protected override Task Given() => Task.CompletedTask;

		protected override async Task When() {
			_descriptionDocument = await GetJson<JObject>(TestStream, "application/vnd.eventstore.streamdesc+json", null);
			_links = _descriptionDocument != null ? _descriptionDocument["_links"].ToList() : new List<JToken>();
		}

		[Fact]
		public void returns_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void returns_a_description_document() {
			Assert.NotNull(_descriptionDocument);
		}

		[Fact]
		public void contains_the_self_link() {
			Assert.Equal("self", ((JProperty)_links[0]).Name);
			Assert.Equal(TestStream, _descriptionDocument["_links"]["self"]["href"].ToString());
		}

		[Fact]
		public void self_link_contains_only_the_description_document_content_type() {
			var supportedContentTypes = _descriptionDocument["_links"]["self"]["supportedContentTypes"].Values<string>()
				.ToArray();
			Assert.Equal(1, supportedContentTypes.Length);
			Assert.Equal("application/vnd.eventstore.streamdesc+json", supportedContentTypes[0]);
		}

		[Fact]
		public void contains_the_stream_link() {
			Assert.Equal("stream", ((JProperty)_links[1]).Name);
			Assert.Equal(TestStream, _descriptionDocument["_links"]["stream"]["href"].ToString());
		}

		[Fact]
		public void stream_link_contains_supported_stream_content_types() {
			var supportedContentTypes = _descriptionDocument["_links"]["stream"]["supportedContentTypes"]
				.Values<string>().ToArray();
			Assert.Equal(2, supportedContentTypes.Length);
			Assert.Contains("application/atom+xml", supportedContentTypes);
			Assert.Contains("application/vnd.eventstore.atom+json", supportedContentTypes);
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_getting_description_document_and_subscription_exists_for_stream : with_admin_user {
		private JObject _descriptionDocument;
		private List<JToken> _links;
		private JToken[] _subscriptions;
		private string _subscriptionUrl;

		protected override async Task Given() {
			_subscriptionUrl = "/subscriptions/" + TestStreamName + "/groupname334";
			await MakeJsonPut(
				_subscriptionUrl,
				new {
					ResolveLinkTos = true
				}, DefaultData.AdminNetworkCredentials);
		}

		protected override async Task When() {
			_descriptionDocument = await GetJson<JObject>(TestStream, "application/vnd.eventstore.streamdesc+json", null);
			_links = _descriptionDocument != null ? _descriptionDocument["_links"].ToList() : new List<JToken>();
			_subscriptions = _descriptionDocument["_links"]["streamSubscription"].Values<JToken>().ToArray();
		}

		[Fact]
		public void returns_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void returns_a_description_document() {
			Assert.NotNull(_descriptionDocument);
		}

		[Fact]
		public void contains_3_links() {
			Assert.Equal(3, _links.Count);
		}

		[Fact]
		public void contains_the_subscription_link() {
			Assert.Equal("streamSubscription", ((JProperty)_links[2]).Name);
			Assert.Equal(_subscriptionUrl, _subscriptions[0]["href"].ToString());
		}

		[Fact]
		public void subscriptions_link_contains_supported_subscription_content_types() {
			var supportedContentTypes = _subscriptions[0]["supportedContentTypes"].Values<string>().ToArray();
			Assert.Equal(2, supportedContentTypes.Length);
			Assert.Contains("application/vnd.eventstore.competingatom+xml", supportedContentTypes);
			Assert.Contains("application/vnd.eventstore.competingatom+json", supportedContentTypes);
		}
	}
}
