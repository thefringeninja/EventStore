using System;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class deleting_existing_persistent_subscription_group_with_permissions : IClassFixture<deleting_existing_persistent_subscription_group_with_permissions.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		private readonly string _stream = Guid.NewGuid().ToString();

		protected override Task When() =>
			Connection.CreatePersistentSubscriptionAsync(_stream, "groupname123", _settings,
				DefaultData.AdminCredentials);

		[Fact]
		public async Task the_delete_of_group_succeeds() {
			await Connection.DeletePersistentSubscriptionAsync(_stream, "groupname123", DefaultData.AdminCredentials);
		}
	}

	[Trait("Category", "LongRunning")]
	public class deleting_existing_persistent_subscription_with_subscriber : IClassFixture<deleting_existing_persistent_subscription_with_subscriber.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		private readonly string _stream = Guid.NewGuid().ToString();
		private readonly ManualResetEvent _called = new ManualResetEvent(false);

		protected override async Task Given() {
			await base.Given();
            await Connection.CreatePersistentSubscriptionAsync(_stream, "groupname123", _settings,
				DefaultData.AdminCredentials);
			Connection.ConnectToPersistentSubscription(_stream, "groupname123",
				(s, e) => Task.CompletedTask,
				(s, r, e) => _called.Set());
		}

		protected override Task When() {
			return Connection.DeletePersistentSubscriptionAsync(_stream, "groupname123", DefaultData.AdminCredentials);
		}

		[Fact]
		public void the_subscription_is_dropped() {
			Assert.True(_called.WaitOne(TimeSpan.FromSeconds(5)));
		}
	}


	[Trait("Category", "LongRunning")]
	public class deleting_persistent_subscription_group_that_doesnt_exist : IClassFixture<deleting_persistent_subscription_group_that_doesnt_exist.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		protected override Task When() => Task.CompletedTask;

		[Fact]
		public Task the_delete_fails_with_argument_exception() {
			return Assert.ThrowsAsync<InvalidOperationException>(
				() =>
					Connection.DeletePersistentSubscriptionAsync(_stream, Guid.NewGuid().ToString(),
						DefaultData.AdminCredentials));
		}
	}


	[Trait("Category", "LongRunning")]
	public class deleting_persistent_subscription_group_without_permissions : IClassFixture<deleting_persistent_subscription_group_without_permissions.Fixture> { public class Fixture : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		protected override Task When() => Task.CompletedTask;

		[Fact]
		public Task the_delete_fails_with_access_denied() {
			return Assert.ThrowsAsync<AccessDeniedException>(
				() => Connection.DeletePersistentSubscriptionAsync(_stream, Guid.NewGuid().ToString()));
		}
	}

//ALL
/*

    [Trait("Category", "LongRunning")]
    public class deleting_existing_persistent_subscription_group_on_all_with_permissions : SpecificationWithMiniNode
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();
        protected override void When()
        {
            _conn.CreatePersistentSubscriptionForAllAsync("groupname123", _settings,
                DefaultData.AdminCredentials).Wait();
        }

        [Fact]
        public void the_delete_of_group_succeeds()
        {
            var result = _conn.DeletePersistentSubscriptionForAllAsync("groupname123", DefaultData.AdminCredentials).Result;
            Assert.Equal(PersistentSubscriptionDeleteStatus.Success, result.Status);
        }
    }

    [Trait("Category", "LongRunning")]
    public class deleting_persistent_subscription_group_on_all_that_doesnt_exist : SpecificationWithMiniNode
    {
        protected override void When()
        {
        }

        [Fact]
        public void the_delete_fails_with_argument_exception()
        {
            try
            {
                _conn.DeletePersistentSubscriptionForAllAsync(Guid.NewGuid().ToString(), DefaultData.AdminCredentials).Wait();
                throw new Exception("expected exception");
            }
            catch (Exception ex)
            {
                Assert.IsType(typeof(AggregateException), ex);
                var inner = ex.InnerException;
                Assert.IsType(typeof(InvalidOperationException), inner);
            }
        }
    }


    [Trait("Category", "LongRunning")]
    public class deleting_persistent_subscription_group_on_all_without_permissions : SpecificationWithMiniNode
    {
        protected override void When()
        {
        }

        [Fact]
        public void the_delete_fails_with_access_denied()
        {
            try
            {
                _conn.DeletePersistentSubscriptionForAllAsync(Guid.NewGuid().ToString()).Wait();
                throw new Exception("expected exception");
            }
            catch (Exception ex)
            {
                Assert.IsType(typeof(AggregateException), ex);
                var inner = ex.InnerException;
                Assert.IsType(typeof(AccessDeniedException), inner);
            }
        }
    }
*/
}
