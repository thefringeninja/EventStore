using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using EventStore.Projections.Core.Tests.ClientAPI.Cluster;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.Transport.Http {
	public class Authorization : IClassFixture<Authorization.Fixture> {
		private readonly Fixture _fixture;

		public static IEnumerable<object[]> TestCases() {
			var userAuthorizationLevels = new[] {
				"None",
				"User",
				"Ops",
				"Admin"
			};

			var useInternalEndpoints = new[] {
				false,
				true
			};

			var endpointDetails = new[] {
				"/web/es/js/projections/{*remaining_path};GET;None",
				"/web/es/js/projections/v8/Prelude/{*remaining_path};GET;None",
				"/web/projections;GET;None",
				"/projections;GET;User",
				"/projections/any;GET;User",
				"/projections/all-non-transient;GET;User",
				"/projections/transient;GET;User",
				"/projections/onetime;GET;User",
				"/projections/continuous;GET;User",
				"/projections/transient?name=name&type=type&enabled={enabled};POST;User", /* /projections/transient?name={name}&type={type}&enabled={enabled} */
				"/projections/onetime?name=name&type=type&enabled={enabled}&checkpoints={checkpoints}&emit={emit}&trackemittedstreams={trackemittedstreams};POST;Ops", /* /projections/onetime?name={name}&type={type}&enabled={enabled}&checkpoints={checkpoints}&emit={emit}&trackemittedstreams={trackemittedstreams} */
				"/projections/continuous?name=name&type=type&enabled={enabled}&emit={emit}&trackemittedstreams={trackemittedstreams};POST;Ops", /* /projections/continuous?name={name}&type={type}&enabled={enabled}&emit={emit}&trackemittedstreams={trackemittedstreams} */
				"/projection/name/query?config={config};GET;User", /* /projection/{name}/query?config={config} */
				"/projection/name/query?type={type}&emit={emit};PUT;User", /* /projection/{name}/query?type={type}&emit={emit} */
				"/projection/name;GET;User", /* /projection/{name} */
				"/projection/name?deleteStateStream={deleteStateStream}&deleteCheckpointStream={deleteCheckpointStream}&deleteEmittedStreams={deleteEmittedStreams};DELETE;Ops", /* /projection/{name}?deleteStateStream={deleteStateStream}&deleteCheckpointStream={deleteCheckpointStream}&deleteEmittedStreams={deleteEmittedStreams} */
				"/projection/name/statistics;GET;User", /* projection/{name}/statistics */
				"/projections/read-events;POST;User",
				"/projection/{name}/state?partition={partition};GET;User",
				"/projection/{name}/result?partition={partition};GET;User",
				"/projection/{name}/command/disable?enableRunAs={enableRunAs};POST;User",
				"/projection/{name}/command/enable?enableRunAs={enableRunAs};POST;User",
				"/projection/{name}/command/reset?enableRunAs={enableRunAs};POST;User",
				"/projection/{name}/command/abort?enableRunAs={enableRunAs};POST;User",
				"/projection/{name}/config;GET;Ops",
				"/projection/{name}/config;PUT;Ops"
				/*"/sys/subsystems;GET;Ops"*/
				/* this endpoint has been commented since this controller is not registered when using a MiniNode */
			};

			foreach (var authLevel in userAuthorizationLevels) {
				foreach (var use in useInternalEndpoints) {
					foreach (var details in endpointDetails) {
						yield return new object[] {
							authLevel, use, details
						};
					}
				}
			}
		}

		public Authorization(Fixture fixture) {
			_fixture = fixture;
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task authorization_tests(
			string userAuthorizationLevel,
			bool useInternalEndpoint,
			string httpEndpointDetails
		) {
			/*use the master node endpoint to avoid any redirects*/
			var nodeEndpoint = useInternalEndpoint
				? _fixture.Nodes[_fixture.MasterId].InternalHttpEndPoint
				: _fixture.Nodes[_fixture.MasterId].ExternalHttpEndPoint;
			var httpEndpointTokens = httpEndpointDetails.Split(';');
			var endpointUrl = httpEndpointTokens[0];
			var httpMethod = GetHttpMethod(httpEndpointTokens[1]);
			var requiredMinAuthorizationLevel = httpEndpointTokens[2];

			var url = string.Format("http://{0}{1}", nodeEndpoint, endpointUrl);
			var body = GetData(httpMethod, endpointUrl);
			var contentType =
				httpMethod == HttpMethod.Post || httpMethod == HttpMethod.Put || httpMethod == HttpMethod.Delete
					? "application/json"
					: null;
			var statusCode =
				await SendRequest(_fixture.HttpClients[userAuthorizationLevel], httpMethod, url, body, contentType);

			if (GetAuthLevel(userAuthorizationLevel) >= GetAuthLevel(requiredMinAuthorizationLevel)) {
				Assert.NotEqual(401, statusCode);
			} else {
				Assert.Equal(401, statusCode);
			}
		}

		private string GetData(HttpMethod httpMethod, string url) =>
			httpMethod == HttpMethod.Post || httpMethod == HttpMethod.Put || httpMethod == HttpMethod.Delete
				? "{}"
				: null;


		private async Task<int> SendRequest(HttpClient client, HttpMethod method, string url, string body,
			string contentType) {
			var request = new HttpRequestMessage();
			request.Method = method;
			request.RequestUri = new Uri(url);

			if (body != null) {
				var bodyBytes = Helper.UTF8NoBom.GetBytes(body);
				var stream = new MemoryStream(bodyBytes);
				var content = new StreamContent(stream);
				content.Headers.ContentLength = bodyBytes.Length;
				if (contentType != null)
					content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
				request.Content = content;
			}

			var result = await client.SendAsync(request);
			return (int)result.StatusCode;
		}

		private HttpMethod GetHttpMethod(string method) {
			switch (method) {
				case "GET":
					return HttpMethod.Get;
				case "POST":
					return HttpMethod.Post;
				case "PUT":
					return HttpMethod.Put;
				case "DELETE":
					return HttpMethod.Delete;
				default:
					throw new Exception("Unknown Http Method");
			}
		}

		private int GetAuthLevel(string userAuthorizationLevel) {
			switch (userAuthorizationLevel) {
				case "None":
					return 0;
				case "User":
					return 1;
				case "Ops":
					return 2;
				case "Admin":
					return 3;
				default:
					throw new Exception("Unknown authorization level");
			}
		}

		public class Fixture : specification_with_standard_projections_runnning {
			public readonly Dictionary<string, HttpClient> HttpClients = new Dictionary<string, HttpClient>();
			private TimeSpan _timeout = TimeSpan.FromSeconds(10);
			public int MasterId;

			private HttpClient CreateHttpClient(string username, string password) {
				var httpClientHandler = new HttpClientHandler();
				httpClientHandler.AllowAutoRedirect = false;

				var client = new HttpClient(httpClientHandler);
				client.Timeout = _timeout;
				client.DefaultRequestHeaders.Authorization =
					new AuthenticationHeaderValue(
						"Basic", System.Convert.ToBase64String(
							System.Text.ASCIIEncoding.ASCII.GetBytes(
								$"{username}:{password}")));

				return client;
			}

			private async Task CreateUser(string username, string password) {
				for (int trial = 1; trial <= 5; trial++) {
					try {
						var dataStr = string.Format(
							"{{loginName: '{0}', fullName: '{1}', password: '{2}', groups: []}}",
							username, username, password);
						var data = Helper.UTF8NoBom.GetBytes(dataStr);
						var stream = new MemoryStream(data);
						var content = new StreamContent(stream);
						content.Headers.Add("Content-Type", "application/json");

						var res = await HttpClients["Admin"].PostAsync(
							string.Format("http://{0}/users/", Nodes[MasterId].ExternalHttpEndPoint),
							content
						);
						res.EnsureSuccessStatusCode();
						break;
					} catch (HttpRequestException) {
						if (trial == 5) {
							throw new Exception(string.Format("Error creating user: {0}", username));
						}

						await Task.Delay(1000);
					}
				}
			}

			protected override async Task Given() {
				await base.Given();
				//find the master node
				for (int i = 0; i < Nodes.Length; i++) {
					if (Nodes[i].NodeState == EventStore.Core.Data.VNodeState.Master) {
						MasterId = i;
						break;
					}
				}

				HttpClients["Admin"] = CreateHttpClient("admin", "changeit");
				HttpClients["Ops"] = CreateHttpClient("ops", "changeit");
				await CreateUser("user", "changeit");
				HttpClients["User"] = CreateHttpClient("user", "changeit");
				HttpClients["None"] = new HttpClient();
			}


			public override Task TestFixtureTearDown() {
				foreach (var kvp in HttpClients) {
					kvp.Value.Dispose();
				}

				return base.TestFixtureTearDown();
			}
		}
	}
}
