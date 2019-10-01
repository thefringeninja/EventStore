using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using EventStore.ClientAPI;
using EventStore.Core.Tests.Http.BasicAuthentication.basic_authentication;
using EventStore.Transport.Http;
using Newtonsoft.Json.Linq;
using Xunit;
using HttpStatusCode = System.Net.HttpStatusCode;
using System.Xml.Linq;
using System.Threading.Tasks;
using EventStore.Core.Tests.Http.Users.users;

namespace EventStore.Core.Tests.Http.PersistentSubscription {
	[Trait("Category", "LongRunning")]
	public class
		when_getting_statistics_for_new_subscription_for_stream_with_existing_events : with_subscription_having_events {
		private JArray _json;

		protected override async Task When() {
			_json = await GetJson<JArray>("/subscriptions", accept: ContentType.Json);
		}

		[Fact]
		public void returns_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void should_reflect_the_known_number_of_events_in_the_stream() {
			var knownNumberOfEvents = _json[0]["lastKnownEventNumber"].Value<int>() + 1;
			Assert.Equal(Events.Count, knownNumberOfEvents);
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_getting_all_statistics_in_json : with_subscription_having_events {
		private JArray _json;

		protected override async Task When() {
			_json = await GetJson<JArray>("/subscriptions", accept: ContentType.Json);
		}

		[Fact]
		public void returns_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void body_contains_valid_json() {
			Assert.Equal(TestStreamName, _json[0]["eventStreamId"].Value<string>());
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_getting_all_statistics_in_xml : with_subscription_having_events {
		private XDocument _xml;

		protected override async Task When() {
			_xml = await GetXml(MakeUrl("/subscriptions"));
		}

		[Fact]
		public void returns_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void body_contains_valid_xml() {
			Assert.Equal(TestStreamName, _xml.Descendants("EventStreamId").First().Value);
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_getting_non_existent_single_statistics : with_admin_user {
		private HttpResponseMessage _response;

		protected override Task Given() => Task.CompletedTask;

		protected override async Task When() {
			var request = CreateRequest("/subscriptions/fu/fubar", null, "GET", "text/xml");
			_response = await GetRequestResponse(request);
		}

		[Fact]
		public void returns_not_found() {
			Assert.Equal(HttpStatusCode.NotFound, _response.StatusCode);
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_getting_non_existent_stream_statistics : with_admin_user {
		private HttpResponseMessage _response;

		protected override Task Given() => Task.CompletedTask;

		protected override async Task When() {
			var request = CreateRequest("/subscriptions/fubar", null, "GET", "text/xml", null);
			_response = await GetRequestResponse(request);
		}

		[Fact]
		public void returns_not_found() {
			Assert.Equal(HttpStatusCode.NotFound, _response.StatusCode);
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		when_getting_subscription_statistics_for_individual : SpecificationWithPersistentSubscriptionAndConnections {
		private JObject _json;


		protected override async Task When() {
			_json = await GetJson<JObject>("/subscriptions/" + _streamName + "/" + _groupName + "/info",
				ContentType.Json);
		}

		[Fact]
		public void returns_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void detail_rel_href_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/subscriptions/{1}/{2}/info", _node.ExtHttpEndPoint, _streamName, _groupName),
				_json["links"][0]["href"].Value<string>());
		}

		[Fact]
		public void has_two_rel_links() {
			Assert.Equal(2,
				_json["links"].Count());
		}

		[Fact]
		public void the_view_detail_rel_is_correct() {
			Assert.Equal("detail",
				_json["links"][0]["rel"].Value<string>());
		}

		[Fact]
		public void the_event_stream_is_correct() {
			Assert.Equal(_streamName, _json["eventStreamId"].Value<string>());
		}

		[Fact]
		public void the_groupname_is_correct() {
			Assert.Equal(_groupName, _json["groupName"].Value<string>());
		}

		[Fact]
		public void the_status_is_live() {
			Assert.Equal("Live", _json["status"].Value<string>());
		}

		[Fact]
		public void there_are_two_connections() {
			Assert.Equal(2, _json["connections"].Count());
		}

		[Fact]
		public void the_first_connection_has_endpoint() {
			Assert.NotNull(_json["connections"][0]["from"]);
		}

		[Fact]
		public void the_second_connection_has_endpoint() {
			Assert.NotNull(_json["connections"][1]["from"]);
		}

		[Fact]
		public void the_first_connection_has_user() {
			Assert.Equal("anonymous", _json["connections"][0]["username"].Value<string>());
		}

		[Fact]
		public void the_second_connection_has_user() {
			Assert.Equal("admin", _json["connections"][1]["username"].Value<string>());
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_getting_subscription_stats_summary : SpecificationWithPersistentSubscriptionAndConnections {
		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		private JArray _json;

		protected override async Task Given() {
			await base.Given();
			await _conn.CreatePersistentSubscriptionAsync(_streamName, "secondgroup", _settings,
				DefaultData.AdminCredentials);
			_conn.ConnectToPersistentSubscription(_streamName, "secondgroup",
				(subscription, @event) => Task.CompletedTask,
				(subscription, reason, arg3) => { });
			_conn.ConnectToPersistentSubscription(_streamName, "secondgroup",
				(subscription, @event) => Task.CompletedTask,
				(subscription, reason, arg3) => { },
				DefaultData.AdminCredentials);
			_conn.ConnectToPersistentSubscription(_streamName, "secondgroup",
				(subscription, @event) => Task.CompletedTask,
				(subscription, reason, arg3) => {
				},
				DefaultData.AdminCredentials);
		}

		protected override async Task When() {
			_json = await GetJson<JArray>("/subscriptions", ContentType.Json);
		}

		[Fact]
		public void the_response_code_is_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void the_first_event_stream_is_correct() {
			Assert.Equal(_streamName, _json[0]["eventStreamId"].Value<string>());
		}

		[Fact]
		public void the_first_groupname_is_correct() {
			Assert.Equal(_groupName, _json[0]["groupName"].Value<string>());
		}

		[Fact]
		public void the_first_event_stream_detail_uri_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/subscriptions/{1}/{2}/info", _node.ExtHttpEndPoint, _streamName, _groupName),
				_json[0]["links"][0]["href"].Value<string>());
		}

		[Fact]
		public void the_first_event_stream_detail_has_one_link() {
			Assert.Equal(1,
				_json[0]["links"].Count());
		}

		[Fact]
		public void the_first_event_stream_detail_rel_is_correct() {
			Assert.Equal("detail",
				_json[0]["links"][0]["rel"].Value<string>());
		}

		[Fact]
		public void the_second_event_stream_detail_uri_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/subscriptions/{1}/{2}/info", _node.ExtHttpEndPoint, _streamName,
					"secondgroup"),
				_json[1]["links"][0]["href"].Value<string>());
		}

		[Fact]
		public void the_second_event_stream_detail_has_one_link() {
			Assert.Equal(1,
				_json[1]["links"].Count());
		}

		[Fact]
		public void the_second_event_stream_detail_rel_is_correct() {
			Assert.Equal("detail",
				_json[1]["links"][0]["rel"].Value<string>());
		}

		[Fact]
		public void the_first_parked_message_queue_uri_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/streams/%24persistentsubscription-{1}::{2}-parked", _node.ExtHttpEndPoint,
					_streamName, _groupName), _json[0]["parkedMessageUri"].Value<string>());
		}

		[Fact]
		public void the_second_parked_message_queue_uri_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/streams/%24persistentsubscription-{1}::{2}-parked", _node.ExtHttpEndPoint,
					_streamName, "secondgroup"), _json[1]["parkedMessageUri"].Value<string>());
		}

		[Fact]
		public void the_status_is_live() {
			Assert.Equal("Live", _json[0]["status"].Value<string>());
		}

		[Fact]
		public void there_are_two_connections() {
			Assert.Equal(2, _json[0]["connectionCount"].Value<int>());
		}

		[Fact]
		public void the_second_subscription_event_stream_is_correct() {
			Assert.Equal(_streamName, _json[1]["eventStreamId"].Value<string>());
		}

		[Fact]
		public void the_second_subscription_groupname_is_correct() {
			Assert.Equal("secondgroup", _json[1]["groupName"].Value<string>());
		}

		[Fact]
		public void second_subscription_there_are_three_connections() {
			Assert.Equal(3, _json[1]["connectionCount"].Value<int>());
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		when_getting_subscription_statistics_for_stream : SpecificationWithPersistentSubscriptionAndConnections {
		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		private JArray _json;
		private EventStorePersistentSubscriptionBase _sub4;
		private EventStorePersistentSubscriptionBase _sub3;
		private EventStorePersistentSubscriptionBase _sub5;

		protected override async Task Given() {
			await base.Given();
			await _conn.CreatePersistentSubscriptionAsync(_streamName, "secondgroup", _settings,
				DefaultData.AdminCredentials);
			_sub3 = _conn.ConnectToPersistentSubscription(_streamName, "secondgroup",
				(subscription, @event) => Task.CompletedTask,
				(subscription, reason, arg3) => { });
			_sub4 = _conn.ConnectToPersistentSubscription(_streamName, "secondgroup",
				(subscription, @event) => Task.CompletedTask,
				(subscription, reason, arg3) => { },
				DefaultData.AdminCredentials);
			_sub5 = _conn.ConnectToPersistentSubscription(_streamName, "secondgroup",
				(subscription, @event) => Task.CompletedTask,
				(subscription, reason, arg3) => { },
				DefaultData.AdminCredentials);
		}

		protected override async Task When() {
			//make mcs stop bitching
			_json = await GetJson<JArray>("/subscriptions/" + _streamName, ContentType.Json);
		}

		[Fact]
		public void the_response_code_is_ok() {
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
		}

		[Fact]
		public void the_first_event_stream_is_correct() {
			Assert.Equal(_streamName, _json[0]["eventStreamId"].Value<string>());
		}

		[Fact]
		public void the_first_groupname_is_correct() {
			Assert.Equal(_groupName, _json[0]["groupName"].Value<string>());
		}

		[Fact]
		public void the_first_event_stream_detail_uri_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/subscriptions/{1}/{2}/info", _node.ExtHttpEndPoint, _streamName, _groupName),
				_json[0]["links"][0]["href"].Value<string>());
		}

		[Fact]
		public void the_second_event_stream_detail_uri_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/subscriptions/{1}/{2}/info", _node.ExtHttpEndPoint, _streamName,
					"secondgroup"),
				_json[1]["links"][0]["href"].Value<string>());
		}

		[Fact]
		public void the_first_parked_message_queue_uri_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/streams/%24persistentsubscription-{1}::{2}-parked", _node.ExtHttpEndPoint,
					_streamName, _groupName), _json[0]["parkedMessageUri"].Value<string>());
		}

		[Fact]
		public void the_second_parked_message_queue_uri_is_correct() {
			Assert.Equal(
				string.Format("http://{0}/streams/%24persistentsubscription-{1}::{2}-parked", _node.ExtHttpEndPoint,
					_streamName, "secondgroup"), _json[1]["parkedMessageUri"].Value<string>());
		}

		[Fact]
		public void the_status_is_live() {
			Assert.Equal("Live", _json[0]["status"].Value<string>());
		}

		[Fact]
		public void there_are_two_connections() {
			Assert.Equal(2, _json[0]["connectionCount"].Value<int>());
		}

		[Fact]
		public void the_second_subscription_event_stream_is_correct() {
			Assert.Equal(_streamName, _json[1]["eventStreamId"].Value<string>());
		}

		[Fact]
		public void the_second_subscription_groupname_is_correct() {
			Assert.Equal("secondgroup", _json[1]["groupName"].Value<string>());
		}

		[Fact]
		public void second_subscription_there_are_three_connections() {
			Assert.Equal(3, _json[1]["connectionCount"].Value<int>());
		}

		public override Task TestFixtureTearDown() {
			_sub3.Stop(TimeSpan.FromMilliseconds(200));
			_sub4.Stop(TimeSpan.FromMilliseconds(200));
			_sub5.Stop(TimeSpan.FromMilliseconds(200));
			return base.TestFixtureTearDown();
		}
	}

	public abstract class SpecificationWithPersistentSubscriptionAndConnections : with_admin_user {
		protected string _streamName = Guid.NewGuid().ToString();
		protected string _groupName = Guid.NewGuid().ToString();
		protected IEventStoreConnection _conn;
		protected EventStorePersistentSubscriptionBase _sub1;
		protected EventStorePersistentSubscriptionBase _sub2;

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override async Task Given() {
			_conn = EventStoreConnection.Create(_node.TcpEndPoint);
			await _conn.ConnectAsync();
			await _conn.CreatePersistentSubscriptionAsync(_streamName, _groupName, _settings,
				DefaultData.AdminCredentials);
			_sub1 = _conn.ConnectToPersistentSubscription(_streamName, _groupName,
				(subscription, @event) => Task.CompletedTask,
				(subscription, reason, arg3) => { });
			_sub2 = _conn.ConnectToPersistentSubscription(_streamName, _groupName,
				(subscription, @event) => Task.CompletedTask,
				(subscription, reason, arg3) => { },
				DefaultData.AdminCredentials);
		}

		protected override Task When() => Task.CompletedTask;

		public override async Task TestFixtureTearDown() {
			_sub1.Stop(TimeSpan.FromMilliseconds(200));
			_sub2.Stop(TimeSpan.FromMilliseconds(200));
			await _conn.DeletePersistentSubscriptionAsync(_streamName, _groupName, DefaultData.AdminCredentials);
			_conn.Close();
			_conn.Dispose();
			await base.TestFixtureTearDown();
		}
	}
}
