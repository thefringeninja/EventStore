﻿using System;
using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI.UserManagement {
	[TestFixture, Category("ClientAPI"), Category("LongRunning")]
	public class enable_disable_user : TestWithUser {
		[Test]
		public void disable_empty_username_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.DisableAsync("", new UserCredentials("admin", "changeit")));
		}

		[Test]
		public void disable_null_username_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.DisableAsync(null, new UserCredentials("admin", "changeit")));
		}

		[Test]
		public void enable_empty_username_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.EnableAsync("", new UserCredentials("admin", "changeit")));
		}

		[Test]
		public void enable_null_username_throws() {
			Assert.ThrowsAsync<ArgumentNullException>(() =>
				_manager.EnableAsync(null, new UserCredentials("admin", "changeit")));
		}

		[Test]
		public async Task can_enable_disable_user() {
            await _manager.DisableAsync(_username, new UserCredentials("admin", "changeit"));

			Assert.ThrowsAsync<UserCommandFailedException>(() =>
				_manager.DisableAsync("foo", new UserCredentials(_username, "password")));

            await _manager.EnableAsync(_username, new UserCredentials("admin", "changeit"));

			var c = await _manager.GetCurrentUserAsync(new UserCredentials(_username, "password"));
		}
	}
}
