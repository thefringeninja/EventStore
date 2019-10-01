using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Xml.Linq;
using EventStore.Core.Tests.Helpers;
using EventStore.Transport.Http;
using Xunit;
using Newtonsoft.Json.Linq;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Tests.Http.Streams;
using EventStore.Core.Tests.Http.Users.users;
using HttpStatusCode = System.Net.HttpStatusCode;

namespace EventStore.Core.Tests.Http.PersistentSubscription {
	public abstract class SpecificationWithLongFeed : with_admin_user {
		protected int _numberOfEvents = 5;

		protected string SubscriptionGroupName {
			get { return "test_subscription_group" + Tag; }
		}

		protected string _subscriptionEndpoint;
		protected string _subscriptionStream;
		protected string _subscriptionGroupName;
		protected List<Guid> _eventIds = new List<Guid>();

		protected async Task SetupPersistentSubscription(string streamId, string groupName, int messageTimeoutInMs = 10000) {
			_subscriptionStream = streamId;
			_subscriptionGroupName = groupName;
			_subscriptionEndpoint =
				String.Format("/subscriptions/{0}/{1}", _subscriptionStream, _subscriptionGroupName);

			var response = await MakeJsonPut(
				_subscriptionEndpoint,
				new {
					ResolveLinkTos = true,
					MessageTimeoutMilliseconds = messageTimeoutInMs
				}, _admin);

			Assert.Equal(HttpStatusCode.Created, response.StatusCode);
		}

		protected async Task<string> PostEvent(int i) {
			var eventId = Guid.NewGuid();
			var response = await MakeArrayEventsPost(
				TestStream, new[] {new {EventId = eventId, EventType = "event-type", Data = new {Number = i}}});
			_eventIds.Add(eventId);
			Assert.Equal(HttpStatusCode.Created, response.StatusCode);
			return response.Headers.Location.ToString();
		}

		protected override async Task Given() {
			await SetupPersistentSubscription(TestStreamName, SubscriptionGroupName);
			for (var i = 0; i < _numberOfEvents; i++) {
				await PostEvent(i);
			}
		}

		protected string GetLink(JObject feed, string relation) {
			var rel = (from JObject link in feed["links"]
				from JProperty attr in link
				where attr.Name == "relation" && (string)attr.Value == relation
				select link).SingleOrDefault();
			return (rel == null) ? (string)null : (string)rel["uri"];
		}
	}

	[Trait("Category", "LongRunning")]
    public class when_retrieving_an_empty_feed : SpecificationWithLongFeed {
		private JObject _feed;
		private JObject _head;
		private string _previous;

		protected override async Task Given() {
			await base.Given();
			_head = await GetJson<JObject>(_subscriptionEndpoint + "/" + _numberOfEvents, ContentType.CompetingJson);
			_previous = GetLink(_head, "previous");
		}

		protected override async Task When() {
			_feed = await GetJson<JObject>(_previous, ContentType.CompetingJson);
		}

		[Fact]
		public void returns_ok_status_code() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void does_not_contain_ack_all_link() {
			var rel = GetLink(_feed, "ackAll");
			Assert.True(string.IsNullOrEmpty(rel));
		}

		[Fact]
		public void does_not_contain_nack_all_link() {
			var rel = GetLink(_feed, "nackAll");
			Assert.True(string.IsNullOrEmpty(rel));
		}

		[Fact]
		public void contains_a_link_rel_previous() {
			var rel = GetLink(_feed, "previous");
			Assert.True(!string.IsNullOrEmpty(rel));
		}

		[Fact]
		public void the_feed_is_empty() {
			Assert.Equal(0, _feed["entries"].Count());
		}
	}

	[Trait("Category", "LongRunning")]
    public class when_retrieving_a_feed_with_events : SpecificationWithLongFeed {
		private JObject _feed;
		private List<JToken> _entries;

		protected override async Task When() {
			var allMessagesFeedLink = String.Format("{0}/{1}", _subscriptionEndpoint, _numberOfEvents);
			_feed = await GetJson<JObject>(allMessagesFeedLink, ContentType.CompetingJson);
			_entries = _feed != null ? _feed["entries"].ToList() : new List<JToken>();
		}

		[Fact]
		public void returns_ok_status_code() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void contains_all_the_events() {
			Assert.Equal(_numberOfEvents, _entries.Count);
		}

		[Fact]
		public void the_ackAll_link_is_to_correct_uri() {
			var ids = String.Format("ids={0}", String.Join(",", _eventIds.ToArray()));
			var ackAllLink = String.Format("subscriptions/{0}/{1}/ack", TestStreamName, SubscriptionGroupName);
			Assert.Equal(MakeUrl(ackAllLink, ids).ToString(), GetLink(_feed, "ackAll"));
		}

		[Fact]
		public void the_nackAll_link_is_to_correct_uri() {
			var ids = String.Format("ids={0}", String.Join(",", _eventIds.ToArray()));
			var nackAllLink = String.Format("subscriptions/{0}/{1}/nack", TestStreamName, SubscriptionGroupName);
			Assert.Equal(MakeUrl(nackAllLink, ids).ToString(), GetLink(_feed, "nackAll"));
		}
	}

	[Trait("Category", "LongRunning")]
    public class when_polling_the_head_forward_and_a_new_event_appears : SpecificationWithLongFeed {
		private JObject _feed;
		private JObject _head;
		private string _previous;
		private string _lastEventLocation;
		private List<JToken> _entries;

		protected override async Task Given() {
			await base.Given();
			_head = await GetJson<JObject>(_subscriptionEndpoint + "/" + _numberOfEvents, ContentType.CompetingJson);
			_previous = GetLink(_head, "previous");
			_lastEventLocation = await PostEvent(-1);
		}

		protected override async Task When() {
			_feed = await GetJson<JObject>(_previous, ContentType.CompetingJson);
			_entries = _feed != null ? _feed["entries"].ToList() : new List<JToken>();
		}

		[Fact]
		public void returns_ok_status_code() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void returns_a_feed_with_a_single_entry_referring_to_the_last_event() {
			HelperExtensions.AssertJson(new {entries = new[] {new {Id = _lastEventLocation}}}, _feed);
		}

		[Fact]
		public void the_ack_link_is_to_correct_uri() {
			var link = _entries[0]["links"][2];
			Assert.Equal("ack", link["relation"].ToString());
			var ackLink = String.Format("subscriptions/{0}/{1}/ack/{2}", TestStreamName, SubscriptionGroupName,
				_eventIds.Last());
			Assert.Equal(MakeUrl(ackLink).ToString(), link["uri"].ToString());
		}

		[Fact]
		public void the_nack_link_is_to_correct_uri() {
			var link = _entries[0]["links"][3];
			Assert.Equal("nack", link["relation"].ToString());
			var ackLink = String.Format("subscriptions/{0}/{1}/nack/{2}", TestStreamName, SubscriptionGroupName,
				_eventIds.Last());
			Assert.Equal(MakeUrl(ackLink).ToString(), link["uri"].ToString());
		}
	}

	[Trait("Category", "LongRunning")]
    public class when_retrieving_a_feed_with_events_with_competing_xml : SpecificationWithLongFeed {
		private XDocument document;
		private XElement[] _entries;

		protected override async Task When() {
			await Get(MakeUrl(_subscriptionEndpoint + "/" + 1).ToString(), String.Empty, ContentType.Competing);
			document = XDocument.Parse(_lastResponseBody);
			_entries = document.GetEntries();
		}

		[Fact]
		public void the_feed_has_n_events() {
			Assert.Equal(1, _entries.Length);
		}

		[Fact]
		public void contains_all_the_events() {
			Assert.Equal(1, _entries.Length);
		}

		[Fact]
		public void the_ackAll_link_is_to_correct_uri() {
			var ids = String.Format("ids={0}", _eventIds[0]);
			var ackAllLink = String.Format("subscriptions/{0}/{1}/ack", TestStreamName, SubscriptionGroupName);
			Assert.Equal(MakeUrl(ackAllLink, ids).ToString(),
				document.Element(XDocumentAtomExtensions.AtomNamespace + "feed").GetLink("ackAll"));
		}

		[Fact]
		public void the_nackAll_link_is_to_correct_uri() {
			var ids = String.Format("ids={0}", _eventIds[0]);
			var nackAllLink = String.Format("subscriptions/{0}/{1}/nack", TestStreamName, SubscriptionGroupName);
			Assert.Equal(MakeUrl(nackAllLink, ids).ToString(),
				document.Element(XDocumentAtomExtensions.AtomNamespace + "feed").GetLink("nackAll"));
		}

		[Fact]
		public void the_ack_link_is_to_correct_uri() {
			var result = document.Element(XDocumentAtomExtensions.AtomNamespace + "feed")
				.Element(XDocumentAtomExtensions.AtomNamespace + "entry")
				.GetLink("ack");
			var ackLink = String.Format("subscriptions/{0}/{1}/ack/{2}", TestStreamName, SubscriptionGroupName,
				_eventIds[0]);
			Assert.Equal(MakeUrl(ackLink).ToString(), result);
		}

		[Fact]
		public void the_nack_link_is_to_correct_uri() {
			var result = document.Element(XDocumentAtomExtensions.AtomNamespace + "feed")
				.Element(XDocumentAtomExtensions.AtomNamespace + "entry")
				.GetLink("nack");
			;
			var nackLink = String.Format("subscriptions/{0}/{1}/nack/{2}", TestStreamName, SubscriptionGroupName,
				_eventIds[0]);
			Assert.Equal(MakeUrl(nackLink).ToString(), result);
		}
	}

	[Trait("Category", "LongRunning")]
    public class when_retrieving_a_feed_with_invalid_content_type : SpecificationWithLongFeed {
		protected override Task When() {
			return Get(MakeUrl(_subscriptionEndpoint + "/" + _numberOfEvents).ToString(), String.Empty, ContentType.Xml);
		}

		[Fact]
		public void returns_not_acceptable() {
			Assert.Equal(HttpStatusCode.NotAcceptable, _lastResponse.StatusCode);
		}
	}

	[Trait("Category", "LongRunning")]
    public class when_retrieving_a_feed_with_events_using_prefix : SpecificationWithLongFeed {
		private JObject _feed;
		private List<JToken> _entries;
		private string _prefix;

		protected override async Task When() {
			_prefix = "myprefix";
			var headers = new NameValueCollection();
			headers.Add("X-Forwarded-Prefix", _prefix);
			var allMessagesFeedLink = String.Format("{0}/{1}", _subscriptionEndpoint, _numberOfEvents);
			_feed = await GetJson<JObject>(allMessagesFeedLink, ContentType.CompetingJson, headers: headers);
			_entries = _feed != null ? _feed["entries"].ToList() : new List<JToken>();
		}

		[Fact]
		public void returns_ok_status_code() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void contains_all_the_events() {
			Assert.Equal(_numberOfEvents, _entries.Count);
		}

		[Fact]
		public void contains_previous_link_with_prefix() {
			var previousLink = String.Format("{0}/subscriptions/{1}/{2}/5", _prefix, TestStreamName,
				SubscriptionGroupName);
			Assert.Equal(MakeUrl(previousLink).ToString(), GetLink(_feed, "previous"));
		}

		[Fact]
		public void contains_self_link_with_prefix() {
			var selfLink = String.Format("{0}/subscriptions/{1}/{2}", _prefix, TestStreamName, SubscriptionGroupName);
			Assert.Equal(MakeUrl(selfLink).ToString(), GetLink(_feed, "self"));
		}

		[Fact]
		public void the_ackAll_link_is_to_correct_uri_with_prefix() {
			var ids = String.Format("ids={0}", String.Join(",", _eventIds.ToArray()));
			var ackAllLink = String.Format("{0}/subscriptions/{1}/{2}/ack", _prefix, TestStreamName,
				SubscriptionGroupName);
			Assert.Equal(MakeUrl(ackAllLink, ids).ToString(), GetLink(_feed, "ackAll"));
		}

		[Fact]
		public void the_nackAll_link_is_to_correct_uri_with_prefix() {
			var ids = String.Format("ids={0}", String.Join(",", _eventIds.ToArray()));
			var nackAllLink = String.Format("{0}/subscriptions/{1}/{2}/nack", _prefix, TestStreamName,
				SubscriptionGroupName);
			Assert.Equal(MakeUrl(nackAllLink, ids).ToString(), GetLink(_feed, "nackAll"));
		}
	}
}
