using System;
using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.ClientAPI.Transport.Http;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class deleting_a_user : TestWithNode {
		[Fact]
		public async Task deleting_non_existing_user_throws() {
			var ex = await Assert.ThrowsAsync<UserCommandFailedException>(() =>
				_manager.DeleteUserAsync(Guid.NewGuid().ToString(), new UserCredentials("admin", "changeit")));
			Assert.Equal(HttpStatusCode.NotFound, ex.HttpStatusCode);
		}

		[Fact]
		public async Task deleting_created_user_deletes_it() {
			var user = Guid.NewGuid().ToString();
			await _manager.CreateUserAsync(user, "ourofull", new[] {"foo", "bar"}, "ouro",
				new UserCredentials("admin", "changeit"));
            await _manager.DeleteUserAsync(user, new UserCredentials("admin", "changeit"));
		}


		[Fact]
		public async Task deleting_null_user_throws() {
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.DeleteUserAsync(null, new UserCredentials("admin", "changeit")));
		}

		[Fact]
		public async Task deleting_empty_user_throws() {
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.DeleteUserAsync("", new UserCredentials("admin", "changeit")));
		}

		[Fact]
		public async Task can_delete_a_user() {
            await _manager.CreateUserAsync("ouro", "ouro", new[] {"foo", "bar"}, "ouro",
				new UserCredentials("admin", "changeit"));
				var x = await _manager.GetUserAsync("ouro", new UserCredentials("admin", "changeit"));
            await _manager.DeleteUserAsync("ouro", new UserCredentials("admin", "changeit"));

			var ex = await Assert.ThrowsAsync<AggregateException>(
				() => _manager.GetUserAsync("ouro", new UserCredentials("admin", "changeit")));
			Assert.Equal(HttpStatusCode.NotFound,
				((UserCommandFailedException)ex.InnerException).HttpStatusCode);
		}
	}
}
