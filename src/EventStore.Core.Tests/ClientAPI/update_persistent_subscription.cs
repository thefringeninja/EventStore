using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class update_existing_persistent_subscription : IClassFixture<update_existing_persistent_subscription.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override async Task Given() {
			await Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any,
				new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
            await Connection.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials);
		}

		protected override Task When() => Task.CompletedTask;

		[Fact]
		public async Task the_completion_succeeds() {
			await Connection.UpdatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials);
		}
	}

	[Trait("Category", "LongRunning")]
	public class update_existing_persistent_subscription_with_subscribers : IClassFixture<update_existing_persistent_subscription_with_subscribers.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		private readonly AutoResetEvent _dropped = new AutoResetEvent(false);
		private SubscriptionDropReason _reason;
		private Exception _exception;
		private Exception _caught = null;

		protected override async Task Given() {
			await Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any,
				new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
            await Connection.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials)
;
			Connection.ConnectToPersistentSubscription(_stream, "existing", (x, y) => Task.CompletedTask,
				(sub, reason, ex) => {
					_dropped.Set();
					_reason = reason;
					_exception = ex;
				});
		}

		protected override async Task When() {
			try {
                await Connection.UpdatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials);
			} catch (Exception ex) {
				_caught = ex;
			}
		}

		[Fact]
		public void the_completion_succeeds() {
			Assert.Null(_caught);
		}

		[Fact]
		public void existing_subscriptions_are_dropped() {
			Assert.True(_dropped.WaitOne(TimeSpan.FromSeconds(5)));
			Assert.Equal(SubscriptionDropReason.UserInitiated, _reason);
			Assert.Null(_exception);
		}
	}


	[Trait("Category", "LongRunning")]
	public class update_non_existing_persistent_subscription : IClassFixture<update_non_existing_persistent_subscription.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override Task When() => Task.CompletedTask;

		[Fact]
		public Task the_completion_fails_with_not_found() {
			return Assert.ThrowsAsync<InvalidOperationException>(
				() => Connection.UpdatePersistentSubscriptionAsync(_stream, "existing", _settings,
					DefaultData.AdminCredentials));
		}
	}

	[Trait("Category", "LongRunning")]
	public class update_existing_persistent_subscription_without_permissions : IClassFixture<update_existing_persistent_subscription_without_permissions.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override async Task When() {
			await Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any,
				new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
            await Connection.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials)
;
		}

		[Fact]
		public Task the_completion_fails_with_access_denied() {
			return Assert.ThrowsAsync<AccessDeniedException>(
				() => Connection.UpdatePersistentSubscriptionAsync(_stream, "existing", _settings, null));
		}
	}
}
