using EventStore.ClientAPI;
using EventStore.ClientAPI.ClientOperations;
using EventStore.ClientAPI.Exceptions;
using Xunit;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "LongRunning"), Trait("Category", "ClientAPI")]
	public class connect_to_non_existing_persistent_subscription_with_permissions_async : SpecificationWithMiniNode {
		private Exception _innerEx;

		protected override async Task When() {
			_innerEx = await Assert.ThrowsAsync<ArgumentException>(() => _conn.ConnectToPersistentSubscriptionAsync(
				"nonexisting2",
				"foo",
				(sub, e) => {
					Console.Write("appeared");
					return Task.CompletedTask;
				},
				(sub, reason, ex) => { }));
		}

		[Fact]
		public void the_subscription_fails_to_connect_with_argument_exception() {
			Assert.IsType<ArgumentException>(_innerEx);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_permissions_async : SpecificationWithMiniNode {
		private EventStorePersistentSubscriptionBase _sub;
		private readonly string _stream = Guid.NewGuid().ToString();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		protected override async Task When() {
            await _conn.CreatePersistentSubscriptionAsync(_stream, "agroupname17", _settings, DefaultData.AdminCredentials)
;
			_sub = await _conn.ConnectToPersistentSubscriptionAsync(_stream,
				"agroupname17",
				(sub, e) => {
					Console.Write("appeared");
					return Task.CompletedTask;
				},
				(sub, reason, ex) => { });
		}

		[Fact]
		public void the_subscription_suceeds() {
			Assert.NotNull(_sub);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_without_permissions_async : SpecificationWithMiniNode {
		private readonly string _stream = "$" + Guid.NewGuid();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		private Exception _innerEx;

		protected override async Task When() {
            await _conn.CreatePersistentSubscriptionAsync(_stream, "agroupname55", _settings,
				DefaultData.AdminCredentials);
			_innerEx = await Assert.ThrowsAsync<AccessDeniedException>(() => _conn.ConnectToPersistentSubscriptionAsync(
				_stream,
				"agroupname55",
				(sub, e) => {
					Console.Write("appeared");
					return Task.CompletedTask;
				},
				(sub, reason, ex) => Console.WriteLine("dropped.")));
		}

		[Fact]
		public void the_subscription_fails_to_connect_with_access_denied_exception() {
			Assert.IsType<AccessDeniedException>(_innerEx);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_max_one_client_async : SpecificationWithMiniNode {
		private readonly string _stream = "$" + Guid.NewGuid();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent()
			.WithMaxSubscriberCountOf(1);

		private Exception _innerEx;

		private const string _group = "startinbeginning1";
		private EventStorePersistentSubscriptionBase _firstConn;

		protected override async Task Given() {
			await base.Given();
            await _conn.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
				DefaultData.AdminCredentials);
			// First connection
			_firstConn = await _conn.ConnectToPersistentSubscriptionAsync(
				_stream,
				_group,
				(s, e) => {
					s.Acknowledge(e);
					return Task.CompletedTask;
				},
				(sub, reason, ex) => { },
				DefaultData.AdminCredentials);
		}

		protected override async Task When() {
			_innerEx = await Assert.ThrowsAsync<MaximumSubscribersReachedException>(() =>
				// Second connection
				_conn.ConnectToPersistentSubscriptionAsync(
					_stream,
					_group,
					(s, e) => {
						s.Acknowledge(e);
						return Task.CompletedTask;
					},
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials));
		}

		[Fact]
		public void the_first_subscription_connects_successfully() {
			Assert.NotNull(_firstConn);
		}

		[Fact]
		public void the_second_subscription_throws_maximum_subscribers_reached_exception() {
			Assert.IsType<MaximumSubscribersReachedException>(_innerEx);
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		connect_to_existing_persistent_subscription_with_start_from_beginning_and_no_stream_async :
			SpecificationWithMiniNode {
		private readonly string _stream = "$" + Guid.NewGuid();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromBeginning();

		private readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);
		private ResolvedEvent _firstEvent;
		private readonly Guid _id = Guid.NewGuid();
		private bool _set = false;

		private const string _group = "startinbeginning1";

		protected override async Task Given() {
            await _conn.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
				DefaultData.AdminCredentials);

            await _conn.ConnectToPersistentSubscriptionAsync(
				_stream,
				_group,
				HandleEvent,
				(sub, reason, ex) => { },
				DefaultData.AdminCredentials);
		}

		protected override Task When() {
			return _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
				new EventData(_id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
		}

		private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
			if (_set) return Task.CompletedTask;
			_set = true;
			_firstEvent = resolvedEvent;
			_resetEvent.Set();
			return Task.CompletedTask;
		}

		[Fact]
		public void the_subscription_gets_event_zero_as_its_first_event() {
			Assert.True(_resetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.Equal(0, _firstEvent.Event.EventNumber);
			Assert.Equal(_id, _firstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		connect_to_existing_persistent_subscription_with_start_from_two_and_no_stream_async :
			SpecificationWithMiniNode {
		private readonly string _stream = "$" + Guid.NewGuid();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFrom(2);

		private readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);
		private ResolvedEvent _firstEvent;
		private readonly Guid _id = Guid.NewGuid();
		private bool _set = false;

		private const string _group = "startinbeginning1";

		protected override async Task Given() {
            await _conn.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
				DefaultData.AdminCredentials);
            await _conn.ConnectToPersistentSubscriptionAsync(
				_stream,
				_group,
				HandleEvent,
				(sub, reason, ex) => { },
				DefaultData.AdminCredentials);
		}

		protected override async Task When() {
            await _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
            await _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
            await _conn.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
				new EventData(_id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
		}

		private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
			if (_set) return Task.CompletedTask;
			_set = true;
			_firstEvent = resolvedEvent;
			_resetEvent.Set();
			return Task.CompletedTask;
		}

		[Fact]
		public void the_subscription_gets_event_two_as_its_first_event() {
			Assert.True(_resetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.Equal(2, _firstEvent.Event.EventNumber);
			Assert.Equal(_id, _firstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		connect_to_existing_persistent_subscription_with_start_from_beginning_and_events_in_it_async :
			SpecificationWithMiniNode {
		private readonly string _stream = "$" + Guid.NewGuid();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromBeginning();

		private readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);
		private ResolvedEvent _firstEvent;
		private List<Guid> _ids = new List<Guid>();
		private bool _set = false;

		private const string _group = "startinbeginning1";

		protected override async Task Given() {
			await WriteEvents(_conn);
            await _conn.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
				DefaultData.AdminCredentials);
		}

		private async Task WriteEvents(IEventStoreConnection connection) {
			for (int i = 0; i < 10; i++) {
				_ids.Add(Guid.NewGuid());
                await connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
						new EventData(_ids[i], "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
			}
		}

		protected override Task When() {
			return _conn.ConnectToPersistentSubscriptionAsync(
				_stream,
				_group,
				HandleEvent,
				(sub, reason, ex) => { },
				DefaultData.AdminCredentials);
		}

		private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
			if (!_set) {
				_set = true;
				_firstEvent = resolvedEvent;
				_resetEvent.Set();
			}

			return Task.CompletedTask;
		}

		[Fact]
		public void the_subscription_gets_event_zero_as_its_first_event() {
			Assert.True(_resetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.Equal(0, _firstEvent.Event.EventNumber);
			Assert.Equal(_ids[0], _firstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		connect_to_existing_persistent_subscription_with_start_from_beginning_not_set_and_events_in_it_async :
			SpecificationWithMiniNode {
		private readonly string _stream = "$" + Guid.NewGuid();

		private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
			.DoNotResolveLinkTos()
			.StartFromCurrent();

		private readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);

		private const string _group = "startinbeginning1";

		protected override async Task Given() {
			await WriteEvents(_conn);
            await _conn.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
				DefaultData.AdminCredentials);
		}

		private async Task WriteEvents(IEventStoreConnection connection) {
			for (int i = 0; i < 10; i++) {
                await connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
						new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"),
							new byte[0]));
			}
		}

		protected override Task When() {
			return _conn.ConnectToPersistentSubscriptionAsync(
				_stream,
				_group,
				HandleEvent,
				(sub, reason, ex) => { },
				DefaultData.AdminCredentials);
		}

		private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
			_resetEvent.Set();
			return Task.CompletedTask;
		}

		[Fact]
		public void the_subscription_gets_no_events() {
			Assert.False(_resetEvent.WaitOne(TimeSpan.FromSeconds(1)));
		}
	}
}
