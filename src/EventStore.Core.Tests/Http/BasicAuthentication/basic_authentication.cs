using System;
using System.Net;
using System.Threading.Tasks;
using EventStore.Core.Services;
using Xunit;
using Newtonsoft.Json.Linq;
using EventStore.Core.Tests.Http.Users.users;

namespace EventStore.Core.Tests.Http.BasicAuthentication {
	namespace basic_authentication {

		[Trait("Category", "LongRunning")]
		public class when_requesting_an_unprotected_resource : with_admin_user {
			protected override Task Given() => Task.CompletedTask;
			protected override async Task When() {
				SetDefaultCredentials(null);
				await GetJson<JObject>("/test-anonymous");
			}

			[Fact]
			public void returns_ok_status_code() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}

			[Fact]
			public void does_not_return_www_authenticate_header() {
				Assert.Empty(_lastResponse.Headers.WwwAuthenticate);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_requesting_a_protected_resource : with_admin_user {
			protected override Task Given() => Task.CompletedTask;

			protected override async Task When() {
				SetDefaultCredentials(null);
				await GetJson<JObject>("/test1");
			}

			[Fact]
			public void returns_unauthorized_status_code() {
				Assert.Equal(HttpStatusCode.Unauthorized, _lastResponse.StatusCode);
			}

			[Fact]
			public void returns_www_authenticate_header() {
				Assert.NotNull(_lastResponse.Headers.WwwAuthenticate);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_requesting_a_protected_resource_with_credentials_provided : with_admin_user {
			protected override async Task Given() {
				var response = await MakeJsonPost(
					"/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
				Assert.Equal(HttpStatusCode.Created, response.StatusCode);
			}

			protected override async Task When() {
				await GetJson<JObject>("/test1", credentials: new NetworkCredential("test1", "Pa55w0rd!"));
			}

			[Fact]
			public void returns_ok_status_code() {
				Assert.Equal(HttpStatusCode.OK, _lastResponse.StatusCode);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_requesting_a_protected_resource_with_invalid_credentials_provided : with_admin_user {
			protected override async Task Given() {
				var response = await MakeJsonPost(
					"/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
				Assert.Equal(HttpStatusCode.Created, response.StatusCode);
			}

			protected override async Task When() {
				await GetJson<JObject>("/test1", credentials: new NetworkCredential("test1", "InvalidPassword!"));
			}

			[Fact]
			public void returns_unauthorized_status_code() {
				Assert.Equal(HttpStatusCode.Unauthorized, _lastResponse.StatusCode);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_requesting_a_protected_resource_with_credentials_of_disabled_user_account : with_admin_user {
			protected override async Task Given() {
				var response = await MakeJsonPost(
					"/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
				Assert.Equal(HttpStatusCode.Created, response.StatusCode);
				response = await MakePost("/users/test1/command/disable", _admin);
				Assert.Equal(HttpStatusCode.OK, response.StatusCode);
			}

			protected override async Task When() {
				await GetJson<JObject>("/test1", credentials: new NetworkCredential("test1", "Pa55w0rd!"));
			}

			[Fact]
			public void returns_unauthorized_status_code() {
				Assert.Equal(HttpStatusCode.Unauthorized, _lastResponse.StatusCode);
			}
		}

		[Trait("Category", "LongRunning")]
		public class when_requesting_a_protected_resource_with_credentials_of_deleted_user_account : with_admin_user {
			protected override async Task Given() {
				var response = await MakeRawJsonPost(
					"/users/", new {LoginName = "test1", FullName = "User Full Name", Password = "Pa55w0rd!"}, _admin);
				Assert.Equal(HttpStatusCode.Created, response.StatusCode);
				Console.WriteLine("done with json post");
				response = await MakeDelete("/users/test1", _admin);
				Assert.Equal(HttpStatusCode.OK, response.StatusCode);
			}

			protected override async Task When() {
				await GetJson<JObject>("/test1", credentials: new NetworkCredential("test1", "Pa55w0rd!"));
			}

			[Fact]
			public void returns_unauthorized_status_code() {
				Assert.Equal(HttpStatusCode.Unauthorized, _lastResponse.StatusCode);
			}
		}
	}
}
