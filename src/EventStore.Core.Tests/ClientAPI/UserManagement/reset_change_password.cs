using System;
using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.ClientAPI.Transport.Http;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement {
	public class reset_password : TestWithUser {
		[Fact]
		public void null_user_name_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ResetPasswordAsync(null, "foo", new UserCredentials("admin", "changeit")));
		}

		[Fact]
		public void empty_user_name_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ResetPasswordAsync("", "foo", new UserCredentials("admin", "changeit")));
		}

		[Fact]
		public void empty_password_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ResetPasswordAsync(_username, "", new UserCredentials("admin", "changeit")));
		}

		[Fact]
		public void null_password_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ResetPasswordAsync(_username, null, new UserCredentials("admin", "changeit")));
		}

		[Fact]
		public async Task can_reset_password() {
            await _manager.ResetPasswordAsync(_username, "foo", new UserCredentials("admin", "changeit"));
			var ex = await Assert.ThrowsAsync<UserCommandFailedException>(
				() => _manager.ChangePasswordAsync(_username, "password", "foobar",
					new UserCredentials(_username, "password"))
			);
			Assert.Equal(HttpStatusCode.Unauthorized,
				ex.HttpStatusCode);
		}
	}

	public class change_password : TestWithUser {
		[Fact]
		public void null_username_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ChangePasswordAsync(null, "oldpassword", "newpassword",
					new UserCredentials("admin", "changeit")));
		}

		[Fact]
		public void empty_username_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ChangePasswordAsync("", "oldpassword", "newpassword", new UserCredentials("admin", "changeit"))
					);
		}

		[Fact]
		public void null_current_password_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ChangePasswordAsync(_username, null, "newpassword", new UserCredentials("admin", "changeit"))
					);
		}

		[Fact]
		public void empty_current_password_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ChangePasswordAsync(_username, "", "newpassword", new UserCredentials("admin", "changeit"))
					);
		}

		[Fact]
		public void null_new_password_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ChangePasswordAsync(_username, "oldpasword", null, new UserCredentials("admin", "changeit"))
					);
		}

		[Fact]
		public void empty_new_password_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.ChangePasswordAsync(_username, "oldpassword", "", new UserCredentials("admin", "changeit"))
					);
		}

		[Fact]
		public async Task can_change_password() {
            await _manager.ChangePasswordAsync(_username, "password", "fubar", new UserCredentials(_username, "password"))
;
			var ex = await Assert.ThrowsAsync<UserCommandFailedException>(
				() => _manager.ChangePasswordAsync(_username, "password", "foobar",
					new UserCredentials(_username, "password"))
			);
			Assert.Equal(HttpStatusCode.Unauthorized,
				ex.HttpStatusCode);
		}
	}
}
