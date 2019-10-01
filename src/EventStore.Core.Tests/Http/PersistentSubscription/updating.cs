using System;
using System.Net;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Tests.Http.Users.users;
using Xunit;

namespace EventStore.Core.Tests.Http.PersistentSubscription {
	[Trait("Category", "LongRunning")]
    public class when_updating_a_subscription_without_permissions : with_admin_user {
		private HttpResponseMessage _response;

		protected override async Task Given() {
			_response = await MakeJsonPut(
				"/subscriptions/stream/groupname337",
				new {
					ResolveLinkTos = true
				}, _admin);
		}

		protected override async Task When() {
			SetDefaultCredentials(null);
			_response = await MakeJsonPost(
				"/subscriptions/stream/groupname337",
				new {
					ResolveLinkTos = true
				}, null);
		}

		[Fact]
		public void returns_unauthorised() {
			Assert.Equal(HttpStatusCode.Unauthorized, _response.StatusCode);
		}
	}

	[Trait("Category", "LongRunning")]
    public class when_updating_a_non_existent_subscription_without_permissions : with_admin_user {
		private HttpResponseMessage _response;

		protected override Task Given() => Task.CompletedTask;

		protected override async Task When() {
			_response = await MakeJsonPost(
				"/subscriptions/stream/groupname3337",
				new {
					ResolveLinkTos = true
				}, new NetworkCredential("admin", "changeit"));
		}

		[Fact]
		public void returns_not_found() {
			Assert.Equal(HttpStatusCode.NotFound, _response.StatusCode);
		}
	}

	[Trait("Category", "LongRunning")]
    public class when_updating_an_existing_subscription : with_admin_user {
		private HttpResponseMessage _response;
		private readonly string _groupName = Guid.NewGuid().ToString();
		private SubscriptionDropReason _droppedReason;
		private Exception _exception;
		private const string _stream = "stream";
		private AutoResetEvent _dropped = new AutoResetEvent(false);

		protected override async Task Given() {
			_response = await MakeJsonPut(
				string.Format("/subscriptions/{0}/{1}", _stream, _groupName),
				new {
					ResolveLinkTos = true
				}, DefaultData.AdminNetworkCredentials);
			SetupSubscription();
		}

		private void SetupSubscription() {
			_connection.ConnectToPersistentSubscription(_stream, _groupName, (x, y) => Task.CompletedTask,
				(sub, reason, ex) => {
					_droppedReason = reason;
					_exception = ex;
					_dropped.Set();
				}, DefaultData.AdminCredentials);
		}

		protected override async Task When() {
			_response = await MakeJsonPost(
				string.Format("/subscriptions/{0}/{1}", _stream, _groupName),
				new {
					ResolveLinkTos = true
				}, DefaultData.AdminNetworkCredentials);
		}

		[Fact]
		public void returns_ok() {
			Assert.Equal(HttpStatusCode.OK, _response.StatusCode);
		}

		[Fact]
		public void existing_subscriptions_are_dropped() {
			Assert.True(_dropped.WaitOne(TimeSpan.FromSeconds(5)));
			Assert.Equal(SubscriptionDropReason.UserInitiated, _droppedReason);
			Assert.Null(_exception);
		}

		[Fact]
		public void location_header_is_present() {
			Assert.Equal(
				string.Format("http://{0}/subscriptions/{1}/{2}", _node.ExtHttpEndPoint, _stream, _groupName),
				_response.Headers.Location.ToString());
		}
	}
}
