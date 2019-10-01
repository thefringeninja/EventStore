using System;
using System.Net;
using System.Text.RegularExpressions;
using EventStore.Core.Tests.Http.Users.users;
using Xunit;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using HttpStatusCode = System.Net.HttpStatusCode;
using EventStore.Transport.Http;

// ReSharper disable InconsistentNaming

namespace EventStore.Core.Tests.Http.PersistentSubscription {
    public class when_nacking_a_message : with_subscription_having_events {
		private HttpResponseMessage _response;
		private string _nackLink;

		protected override async Task Given() {
			await base.Given();
			var json = await GetJson<JObject>(
				SubscriptionPath + "/1",
				ContentType.CompetingJson,
				_admin);
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			_nackLink = json["entries"].Children().First()["links"].Children()
				.First(x => x.Value<string>("relation") == "nack").Value<string>("uri");
		}

		protected override async Task When() {
			_response = await MakePost(_nackLink, _admin);
		}

		[Fact]
		public void returns_accepted() {
			Assert.Equal(HttpStatusCode.Accepted, _response.StatusCode);
		}
	}

    public class when_nacking_messages : with_subscription_having_events {
		private HttpResponseMessage _response;
		private string _nackAllLink;

		protected override async Task Given() {
			await base.Given();
			var json = await GetJson<JObject>(
				SubscriptionPath + "/" + Events.Count,
				ContentType.CompetingJson,
				_admin);
			Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			_nackAllLink = json["links"].Children().First(x => x.Value<string>("relation") == "nackAll")
				.Value<string>("uri");
		}

		protected override async Task When() {
			_response = await MakePost(_nackAllLink, _admin);
		}

		[Fact]
		public void returns_accepted() {
			Assert.Equal(HttpStatusCode.Accepted, _response.StatusCode);
		}
	}
}
