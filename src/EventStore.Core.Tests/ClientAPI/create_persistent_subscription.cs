using System;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class create_persistent_subscription_on_existing_stream : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override Task When() {
			return _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any,
				new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
		}

		[Fact]
		public async Task the_completion_succeeds() {
			await _conn.CreatePersistentSubscriptionAsync(_stream, "existing", _settings,
				DefaultData.AdminCredentials);
		}
	}


	[Trait("Category", "LongRunning")]
	public class create_persistent_subscription_on_non_existing_stream : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override Task When() => Task.CompletedTask;

		[Fact]
		public async Task the_completion_succeeds() {
			await _conn.CreatePersistentSubscriptionAsync(_stream, "nonexistinggroup", _settings,
				DefaultData.AdminCredentials);
		}
	}


	[Trait("Category", "LongRunning")]
	public class create_persistent_subscription_on_all_stream : SpecificationWithMiniNode {
		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override Task When() => Task.CompletedTask;

		[Fact]
		public void the_completion_fails_with_invalid_stream() {
			Assert.ThrowsAsync<InvalidOperationException>(() =>
				_conn.CreatePersistentSubscriptionAsync("$all", "shitbird", _settings, DefaultData.AdminCredentials));
		}
	}


	[Trait("Category", "LongRunning")]
	public class create_persistent_subscription_with_too_big_message_timeout : SpecificationWithMiniNode {
		protected override Task When() => Task.CompletedTask;

		[Fact]
		public void the_build_fails_with_argument_exception() {
			Assert.Throws<ArgumentException>(() =>
				PersistentSubscriptionSettings.Create().WithMessageTimeoutOf(TimeSpan.FromDays(25 * 365)).Build());
		}
	}


	[Trait("Category", "LongRunning")]
	public class create_persistent_subscription_with_too_big_checkpoint_after : SpecificationWithMiniNode {
		protected override Task When() => Task.CompletedTask;

		[Fact]
		public void the_build_fails_with_argument_exception() {
			Assert.Throws<ArgumentException>(() =>
				PersistentSubscriptionSettings.Create().CheckPointAfter(TimeSpan.FromDays(25 * 365)).Build());
		}
	}

	[Trait("Category", "LongRunning")]
	public class create_persistent_subscription_with_dont_timeout : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent()
			.DontTimeoutMessages();

		protected override Task When() => Task.CompletedTask;

		[Fact]
		public void the_message_timeout_should_be_zero() {
			Assert.True(_settings.MessageTimeout == TimeSpan.Zero);
		}

		[Fact]
		public async Task the_subscription_is_created_without_error() {
			await _conn.CreatePersistentSubscriptionAsync(_stream, "dont-timeout", _settings,
				DefaultData.AdminCredentials);
		}
	}

	[Trait("Category", "LongRunning")]
	public class create_duplicate_persistent_subscription_group : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override Task When() {
			return _conn.CreatePersistentSubscriptionAsync(_stream, "group32", _settings, DefaultData.AdminCredentials);
		}

		[Fact]
		public void the_completion_fails_with_invalid_operation_exception() {
			Assert.ThrowsAsync<InvalidOperationException>(
				() => _conn.CreatePersistentSubscriptionAsync(_stream, "group32", _settings,
					DefaultData.AdminCredentials));
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		can_create_duplicate_persistent_subscription_group_name_on_different_streams : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override Task When() {
			return _conn.CreatePersistentSubscriptionAsync(_stream, "group3211", _settings, DefaultData.AdminCredentials);
		}

		[Fact]
		public async Task the_completion_succeeds() {
			await
				_conn.CreatePersistentSubscriptionAsync("someother" + _stream, "group3211", _settings,
					DefaultData.AdminCredentials);
		}
	}

	[Trait("Category", "LongRunning")]
	public class create_persistent_subscription_group_without_permissions : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override Task When() => Task.CompletedTask;

		[Fact]
		public void the_completion_succeeds() {
			Assert.ThrowsAsync<AccessDeniedException>(() =>
				_conn.CreatePersistentSubscriptionAsync(_stream, "group57", _settings, null));
		}
	}


	[Trait("Category", "LongRunning")]
	public class create_persistent_subscription_after_deleting_the_same : SpecificationWithMiniNode {
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override async Task When() {
			await _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any,
				new EventData(Guid.NewGuid(), "whatever", true, Encoding.UTF8.GetBytes("{'foo' : 2}"), new Byte[0]));
            await _conn.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials);
            await _conn.DeletePersistentSubscriptionAsync(_stream, "existing", DefaultData.AdminCredentials);
		}

		[Fact]
		public async Task the_completion_succeeds() {
            await _conn.CreatePersistentSubscriptionAsync(_stream, "existing", _settings, DefaultData.AdminCredentials);
		}
	}

//ALL
/*

    [Trait("Category", "LongRunning")]
    public class create_persistent_subscription_on_all : SpecificationWithMiniNode
    {
        private PersistentSubscriptionCreateResult _result;
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();

        protected override void When()
        {
            _result = _conn.CreatePersistentSubscriptionForAllAsync("group", _settings, DefaultData.AdminCredentials).Result;
        }

        [Fact]
        public void the_completion_succeeds()
        {
            Assert.Equal(PersistentSubscriptionCreateStatus.Success, _result.Status);
        }
    }


    [Trait("Category", "LongRunning")]
    public class create_duplicate_persistent_subscription_group_on_all : SpecificationWithMiniNode
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();
        protected override void When()
        {
            _conn.CreatePersistentSubscriptionForAllAsync("group32", _settings, DefaultData.AdminCredentials).Wait();
        }

        [Fact]
        public void the_completion_fails_with_invalid_operation_exception()
        {
            try
            {
                _conn.CreatePersistentSubscriptionForAllAsync("group32", _settings, DefaultData.AdminCredentials).Wait();
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
    public class create_persistent_subscription_group_on_all_without_permissions : SpecificationWithMiniNode
    {
        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
                                                                .DoNotResolveLinkTos()
                                                                .StartFromCurrent();
        protected override void When()
        {
        }

        [Fact]
        public void the_completion_succeeds()
        {
            try
            {
                _conn.CreatePersistentSubscriptionForAllAsync("group57", _settings, null).Wait();
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
