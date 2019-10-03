using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.ClientOperations;
using EventStore.ClientAPI.Exceptions;
using Xunit;
using EventStore.ClientAPI.Common;
using EventStore.ClientAPI.Common.Utils;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "LongRunning"), Trait("Category", "ClientAPI")]
	public class connect_to_non_existing_persistent_subscription_with_permissions :
		IClassFixture<connect_to_non_existing_persistent_subscription_with_permissions.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			public Exception Caught;

			protected override async Task When() {
				Caught = (await Assert.ThrowsAsync<AggregateException>(
					() => {
						Connection.ConnectToPersistentSubscription(
							"nonexisting2",
							"foo",
							(sub, e) => {
								Console.Write("appeared");
								return Task.CompletedTask;
							},
							(sub, reason, ex) => { });
						throw new Exception("should have thrown");
					})).InnerException;
			}
		}

		public connect_to_non_existing_persistent_subscription_with_permissions(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public void the_completion_fails() {
			Assert.NotNull(_fixture.Caught);
		}

		[Fact]
		public void the_exception_is_an_argument_exception() {
			Assert.IsType<ArgumentException>(_fixture.Caught);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_permissions :
		IClassFixture<connect_to_existing_persistent_subscription_with_permissions.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			public EventStorePersistentSubscriptionBase Sub;
			private readonly string _stream = Guid.NewGuid().ToString();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFromCurrent();

			protected override async Task When() {
				await Connection.CreatePersistentSubscriptionAsync(_stream, "agroupname17", _settings,
						DefaultData.AdminCredentials)
					;
				Sub = Connection.ConnectToPersistentSubscription(_stream,
					"agroupname17",
					(sub, e) => {
						Console.Write("appeared");
						return Task.CompletedTask;
					},
					(sub, reason, ex) => { });
			}
		}

		public connect_to_existing_persistent_subscription_with_permissions(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public void the_subscription_suceeds() {
			Assert.NotNull(_fixture.Sub);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_without_permissions :
		IClassFixture<connect_to_existing_persistent_subscription_without_permissions.Fixture> {
		private readonly Fixture _fixture;
		private readonly ITestOutputHelper _testOutputHelper;

		public class Fixture : SpecificationWithMiniNode {
			public readonly string Stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFromCurrent();

			protected override Task When() {
				return Connection.CreatePersistentSubscriptionAsync(Stream, "agroupname55", _settings,
					DefaultData.AdminCredentials);
			}
		}

		public connect_to_existing_persistent_subscription_without_permissions(Fixture fixture,
			ITestOutputHelper testOutputHelper) {
			_fixture = fixture;
			_testOutputHelper = testOutputHelper;
		}

		[Fact]
		public void the_subscription_fails_to_connect() {
			try {
				_fixture.Connection.ConnectToPersistentSubscription(
					_fixture.Stream,
					"agroupname55",
					(sub, e) => {
						_testOutputHelper.WriteLine("appeared");
						return Task.CompletedTask;
					},
					(sub, reason, ex) => _testOutputHelper.WriteLine("dropped."));
				throw new Exception("should have thrown.");
			} catch (Exception ex) {
				Assert.IsType<AggregateException>(ex);
				Assert.IsType<AccessDeniedException>(ex.InnerException);
			}
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_max_one_client :
		IClassFixture<connect_to_existing_persistent_subscription_with_max_one_client.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFromCurrent()
				.WithMaxSubscriberCountOf(1);

			public Exception Exception;

			private const string _group = "startinbeginning1";

			protected override async Task Given() {
				await base.Given();
				await Connection.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
					DefaultData.AdminCredentials);
				Connection.ConnectToPersistentSubscription(
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
				Exception = (await Assert.ThrowsAsync<AggregateException>(() => {
					Connection.ConnectToPersistentSubscription(
						_stream,
						_group,
						(s, e) => {
							s.Acknowledge(e);
							return Task.CompletedTask;
						},
						(sub, reason, ex) => { },
						DefaultData.AdminCredentials);
					throw new Exception("should have thrown.");
				})).InnerException;
			}
		}

		public connect_to_existing_persistent_subscription_with_max_one_client(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public void the_second_subscription_fails_to_connect() {
			Assert.IsType<MaximumSubscribersReachedException>(_fixture.Exception);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_start_from_beginning_and_no_stream :
		IClassFixture<connect_to_existing_persistent_subscription_with_start_from_beginning_and_no_stream.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFromBeginning();

			public readonly Guid Id = Guid.NewGuid();

			public readonly TaskCompletionSource<ResolvedEvent> FirstEventSource =
				new TaskCompletionSource<ResolvedEvent>(TaskCreationOptions.RunContinuationsAsynchronously);

			private const string Group = "startinbeginning1";

			protected override async Task Given() {
				await Connection.CreatePersistentSubscriptionAsync(_stream, Group, _settings,
					DefaultData.AdminCredentials);
				Connection.ConnectToPersistentSubscription(
					_stream,
					Group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials);
			}

			protected override Task When() {
				return Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				FirstEventSource.TrySetResult(resolvedEvent);
				return Task.CompletedTask;
			}
		}

		public connect_to_existing_persistent_subscription_with_start_from_beginning_and_no_stream(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public async Task the_subscription_gets_event_zero_as_its_first_event() {
			var firstEvent = await _fixture.FirstEventSource.Task.WithTimeout(TimeSpan.FromSeconds(10));
			Assert.Equal(0, firstEvent.Event.EventNumber);
			Assert.Equal(_fixture.Id, firstEvent.Event.EventId);
		}
	}


	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_start_from_two_and_no_stream :
		IClassFixture<connect_to_existing_persistent_subscription_with_start_from_two_and_no_stream> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFrom(2);

			private readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);
			public readonly Guid Id = Guid.NewGuid();

			public readonly TaskCompletionSource<ResolvedEvent> FirstEventSource =
				new TaskCompletionSource<ResolvedEvent>(TaskCreationOptions.RunContinuationsAsynchronously);

			private const string _group = "startinbeginning1";

			protected override async Task Given() {
				await Connection.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
					DefaultData.AdminCredentials);
				Connection.ConnectToPersistentSubscription(
					_stream,
					_group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials);
			}

			protected override async Task When() {
				await Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"),
						new byte[0]));
				await Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"),
						new byte[0]));
				await Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				FirstEventSource.TrySetResult(resolvedEvent);
				return Task.CompletedTask;
			}
		}

		public connect_to_existing_persistent_subscription_with_start_from_two_and_no_stream(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public async Task the_subscription_gets_event_two_as_its_first_event() {
			var resolvedEvent = await _fixture.FirstEventSource.Task.WithTimeout(TimeSpan.FromSeconds(10));
			Assert.Equal(2, resolvedEvent.Event.EventNumber);
			Assert.Equal(_fixture.Id, resolvedEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_start_from_beginning_and_events_in_it :
		IClassFixture<connect_to_existing_persistent_subscription_with_start_from_beginning_and_events_in_it.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFromBeginning();

			public readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);
			public ResolvedEvent FirstEvent;
			public List<Guid> Ids = new List<Guid>();
			private bool _set = false;

			private const string Group = "startinbeginning1";

			protected override async Task Given() {
				await WriteEvents(Connection);
				await Connection.CreatePersistentSubscriptionAsync(_stream, Group, _settings,
					DefaultData.AdminCredentials);
			}

			private async Task WriteEvents(IEventStoreConnection connection) {
				for (int i = 0; i < 10; i++) {
					Ids.Add(Guid.NewGuid());
					await connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
							new EventData(Ids[i], "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"),
								new byte[0]));
				}
			}

			protected override Task When() {
				Connection.ConnectToPersistentSubscription(
					_stream,
					Group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials);
				return Task.CompletedTask;
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				if (!_set) {
					_set = true;
					FirstEvent = resolvedEvent;
					_resetEvent.Set();
				}

				return Task.CompletedTask;
			}
		}

		public connect_to_existing_persistent_subscription_with_start_from_beginning_and_events_in_it(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public void the_subscription_gets_event_zero_as_its_first_event() {
			Assert.True(_fixture._resetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.Equal(0, _fixture.FirstEvent.Event.EventNumber);
			Assert.Equal(_fixture.Ids[0], _fixture.FirstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_start_from_beginning_not_set_and_events_in_it :
		IClassFixture<connect_to_existing_persistent_subscription_with_start_from_beginning_not_set_and_events_in_it.
			Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFromCurrent();

			public readonly AutoResetEvent ResetEvent = new AutoResetEvent(false);

			private const string _group = "startinbeginning1";

			protected override async Task Given() {
				await WriteEvents(Connection);
				await Connection.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
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
				Connection.ConnectToPersistentSubscription(
					_stream,
					_group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials);
				return Task.CompletedTask;
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				ResetEvent.Set();
				return Task.CompletedTask;
			}
		}

		public connect_to_existing_persistent_subscription_with_start_from_beginning_not_set_and_events_in_it(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public void the_subscription_gets_no_events() {
			Assert.False(_fixture.ResetEvent.WaitOne(TimeSpan.FromSeconds(1)));
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		connect_to_existing_persistent_subscription_with_start_from_beginning_not_set_and_events_in_it_then_event_written :
			IClassFixture<
				connect_to_existing_persistent_subscription_with_start_from_beginning_not_set_and_events_in_it_then_event_written
				.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos();

			public readonly AutoResetEvent ResetEvent = new AutoResetEvent(false);
			public ResolvedEvent FirstEvent;
			public Guid Id;

			private const string _group = "startinbeginning1";

			protected override async Task Given() {
				await WriteEvents(Connection);
				await Connection.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
					DefaultData.AdminCredentials);
				Connection.ConnectToPersistentSubscription(
					_stream,
					_group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials);
			}

			private async Task WriteEvents(IEventStoreConnection connection) {
				for (int i = 0; i < 10; i++) {
					await connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
						new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"),
							new byte[0]));
				}
			}

			protected override async Task When() {
				Id = Guid.NewGuid();
				await Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				FirstEvent = resolvedEvent;
				ResetEvent.Set();
				return Task.CompletedTask;
			}
		}

		public connect_to_existing_persistent_subscription_with_start_from_beginning_not_set_and_events_in_it_then_event_written(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public void the_subscription_gets_the_written_event_as_its_first_event() {
			Assert.True(_fixture.ResetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.NotNull(_fixture.FirstEvent);
			Assert.Equal(10, _fixture.FirstEvent.Event.EventNumber);
			Assert.Equal(_fixture.Id, _fixture.FirstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		connect_to_existing_persistent_subscription_with_start_from_x_set_higher_than_x_and_events_in_it_then_event_written :
			IClassFixture<
				connect_to_existing_persistent_subscription_with_start_from_x_set_higher_than_x_and_events_in_it_then_event_written
				.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFrom(11);

			public readonly AutoResetEvent ResetEvent = new AutoResetEvent(false);
			public ResolvedEvent FirstEvent;
			public Guid Id;

			private const string _group = "startinbeginning1";

			protected override async Task Given() {
				await WriteEvents(Connection);
				await Connection.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
					DefaultData.AdminCredentials);
				Connection.ConnectToPersistentSubscription(
					_stream,
					_group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials);
			}

			private async Task WriteEvents(IEventStoreConnection connection) {
				for (int i = 0; i < 11; i++) {
					await connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
							new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"),
								new byte[0]));
				}
			}

			protected override Task When() {
				Id = Guid.NewGuid();
				return Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				FirstEvent = resolvedEvent;
				ResetEvent.Set();
				return Task.CompletedTask;
			}
		}
		public connect_to_existing_persistent_subscription_with_start_from_x_set_higher_than_x_and_events_in_it_then_event_written(Fixture fixture) {
			_fixture = fixture;
		}


		[Fact]
		public void the_subscription_gets_the_written_event_as_its_first_event() {
			Assert.True(_fixture.ResetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.NotNull(_fixture.FirstEvent);
			Assert.Equal(11, _fixture.FirstEvent.Event.EventNumber);
			Assert.Equal(_fixture.Id, _fixture.FirstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class a_nak_in_subscription_handler_in_autoack_mode_drops_the_subscription :
		IClassFixture<a_nak_in_subscription_handler_in_autoack_mode_drops_the_subscription.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFromBeginning();

			public readonly ManualResetEvent ResetEvent = new ManualResetEvent(false);
			public Exception Exception;
			public SubscriptionDropReason Reason;

			private const string Group = "naktest";

			protected override async Task Given() {
				await Connection.CreatePersistentSubscriptionAsync(_stream, Group, _settings,
					DefaultData.AdminCredentials);
				Connection.ConnectToPersistentSubscription(
					_stream,
					Group,
					HandleEvent,
					Dropped,
					DefaultData.AdminCredentials);
			}

			private void Dropped(EventStorePersistentSubscriptionBase sub, SubscriptionDropReason reason,
				Exception exception) {
				Exception = exception;
				Reason = reason;
				ResetEvent.Set();
			}

			protected override Task When() {
				return Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Guid.NewGuid(), "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"),
						new byte[0]));
			}

			private static Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				throw new Exception("test");
			}
		}
		
		public a_nak_in_subscription_handler_in_autoack_mode_drops_the_subscription(Fixture fixture) {
			_fixture = fixture;
		}


		[Fact]
		public void the_subscription_gets_dropped() {
			Assert.True(_fixture.ResetEvent.WaitOne(TimeSpan.FromSeconds(5)));
			Assert.Equal(SubscriptionDropReason.EventHandlerException, _fixture.Reason);
			Assert.Equal(typeof(Exception), _fixture.Exception.GetType());
			Assert.Equal("test", _fixture.Exception.Message);
		}
	}


	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_start_from_x_set_and_events_in_it_then_event_written :
		IClassFixture<
			connect_to_existing_persistent_subscription_with_start_from_x_set_and_events_in_it_then_event_written.
			Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFrom(10);

			public readonly AutoResetEvent ResetEvent = new AutoResetEvent(false);
			public ResolvedEvent FirstEvent;
			public Guid Id;

			private const string _group = "startinbeginning1";

			protected override async Task Given() {
				await WriteEvents(Connection);
				await Connection.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
					DefaultData.AdminCredentials);
				Connection.ConnectToPersistentSubscription(
					_stream,
					_group,
					HandleEvent,
					(sub, reason, ex) => { },
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
				Id = Guid.NewGuid();
				return Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				FirstEvent = resolvedEvent;
				ResetEvent.Set();
				return Task.CompletedTask;
			}
		}
		
		public connect_to_existing_persistent_subscription_with_start_from_x_set_and_events_in_it_then_event_written(Fixture fixture) {
			_fixture = fixture;
		}


		[Fact]
		public void the_subscription_gets_the_written_event_as_its_first_event() {
			Assert.True(_fixture.ResetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.NotNull(_fixture.FirstEvent);
			Assert.Equal(10, _fixture.FirstEvent.Event.EventNumber);
			Assert.Equal(_fixture.Id, _fixture.FirstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class connect_to_existing_persistent_subscription_with_start_from_x_set_and_events_in_it : IClassFixture<
		connect_to_existing_persistent_subscription_with_start_from_x_set_and_events_in_it.Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = "$" + Guid.NewGuid();

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFrom(4);

			public readonly AutoResetEvent ResetEvent = new AutoResetEvent(false);
			public ResolvedEvent FirstEvent;
			public Guid Id;

			private const string Group = "startinx2";

			protected override async Task Given() {
				await WriteEvents(Connection);
				await Connection.CreatePersistentSubscriptionAsync(_stream, Group, _settings,
					DefaultData.AdminCredentials);
				Connection.ConnectToPersistentSubscription(
					_stream,
					Group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials);
			}

			private async Task WriteEvents(IEventStoreConnection connection) {
				for (int i = 0; i < 10; i++) {
					var id = Guid.NewGuid();
					await connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
						new EventData(id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
					if (i == 4) Id = id;
				}
			}

			protected override Task When() {
				return Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(Id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
			}

			private bool _set = false;

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				if (_set) return Task.CompletedTask;
				_set = true;
				FirstEvent = resolvedEvent;
				ResetEvent.Set();
				return Task.CompletedTask;
			}
		}

		
		public connect_to_existing_persistent_subscription_with_start_from_x_set_and_events_in_it(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public void the_subscription_gets_the_written_event_as_its_first_event() {
			Assert.True(_fixture.ResetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.NotNull(_fixture.FirstEvent);
			Assert.Equal(4, _fixture.FirstEvent.Event.EventNumber);
			Assert.Equal(_fixture.Id, _fixture.FirstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		connect_to_persistent_subscription_with_link_to_event_with_event_number_greater_than_int_maxvalue :
			IClassFixture<
				connect_to_persistent_subscription_with_link_to_event_with_event_number_greater_than_int_maxvalue.
				Fixture> {
		private readonly Fixture _fixture;

		public class Fixture : ExpectedVersion64Bit.MiniNodeWithExistingRecords {
			private const string StreamName =
				"connect_to_persistent_subscription_with_link_to_event_with_event_number_greater_than_int_maxvalue";

			public const long intMaxValue = (long)int.MaxValue;

			private string _linkedStreamName = "linked-" + StreamName;
			private const string Group = "group-" + StreamName;

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.ResolveLinkTos()
				.StartFromBeginning();

			public readonly AutoResetEvent ResetEvent = new AutoResetEvent(false);
			public ResolvedEvent FirstEvent;
			private bool _set = false;
			public Guid Event1Id;

			public override void WriteTestScenario() {
				var event1 = WriteSingleEvent(StreamName, intMaxValue + 1, new string('.', 3000));
				WriteSingleEvent(StreamName, intMaxValue + 2, new string('.', 3000));
				Event1Id = event1.EventId;
			}

			public override async Task Given() {
				_store = BuildConnection(Node);
				await _store.ConnectAsync();

				await _store.CreatePersistentSubscriptionAsync(_linkedStreamName, Group, _settings,
					DefaultData.AdminCredentials);
				_store.ConnectToPersistentSubscription(
					_linkedStreamName,
					Group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials);
				await _store.AppendToStreamAsync(_linkedStreamName, ExpectedVersion.Any, new EventData(Guid.NewGuid(),
					SystemEventTypes.LinkTo, false, Helper.UTF8NoBom.GetBytes(
						string.Format("{0}@{1}", intMaxValue + 1, StreamName)), null));
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent) {
				if (!_set) {
					_set = true;
					FirstEvent = resolvedEvent;
					ResetEvent.Set();
				}

				return Task.CompletedTask;
			}
		}
		
		public connect_to_persistent_subscription_with_link_to_event_with_event_number_greater_than_int_maxvalue(Fixture fixture) {
			_fixture = fixture;
		}

		[Fact]
		public void the_subscription_resolves_the_linked_event_correctly() {
			Assert.True(_fixture.ResetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.Equal(Fixture.intMaxValue + 1, _fixture.FirstEvent.Event.EventNumber);
			Assert.Equal(_fixture.Event1Id, _fixture.FirstEvent.Event.EventId);
		}
	}

	[Trait("Category", "LongRunning")]
	public class
		connect_to_persistent_subscription_with_retries : IClassFixture<
			connect_to_persistent_subscription_with_retries.Fixture> {
		public class Fixture : SpecificationWithMiniNode {
			private readonly string _stream = Guid.NewGuid().ToString("N");

			private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettings.Create()
				.DoNotResolveLinkTos()
				.StartFromBeginning();

			private readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);
			private readonly Guid _id = Guid.NewGuid();
			int? _retryCount;
			private const string _group = "retries";

			protected override async Task Given() {
				await Connection.CreatePersistentSubscriptionAsync(_stream, _group, _settings,
					DefaultData.AdminCredentials);
				await Connection.ConnectToPersistentSubscriptionAsync(
					_stream,
					_group,
					HandleEvent,
					(sub, reason, ex) => { },
					DefaultData.AdminCredentials, autoAck: false);
			}

			protected override Task When() {
				return Connection.AppendToStreamAsync(_stream, ExpectedVersion.Any, DefaultData.AdminCredentials,
					new EventData(_id, "test", true, Encoding.UTF8.GetBytes("{'foo' : 'bar'}"), new byte[0]));
			}

			private Task HandleEvent(EventStorePersistentSubscriptionBase sub, ResolvedEvent resolvedEvent,
				int? retryCount) {
				if (retryCount > 4) {
					_retryCount = retryCount;
					sub.Acknowledge(resolvedEvent);
					_resetEvent.Set();
				} else {
					sub.Fail(resolvedEvent, PersistentSubscriptionNakEventAction.Retry,
						"Not yet tried enough times");
				}

				return Task.CompletedTask;
			}

			[Fact]
			public void events_are_retried_until_success() {
				Assert.True(_resetEvent.WaitOne(TimeSpan.FromSeconds(10)));
				Assert.Equal(5, _retryCount);
			}
		}
	}
	//ALL

	/*

	    [Trait("Category", "LongRunning")]
	    public class connect_to_non_existing_persistent_all_subscription_with_permissions : SpecificationWithMiniNode
	    {
	        private Exception _caught;

	        protected override void When()
	        {
	            try
	            {
	                _conn.ConnectToPersistentSubscriptionForAll("nonexisting2",
	                    (sub, e) => Console.Write("appeared"),
	                    (sub, reason, ex) =>
	                    {
	                    }, 
	                    DefaultData.AdminCredentials);
	                throw new Exception("should have thrown");
	            }
	            catch (Exception ex)
	            {
	                _caught = ex;
	            }
	        }

	        [Fact]
	        public void the_completion_fails()
	        {
	            Assert.NotNull(_caught);
	        }

	        [Fact]
	        public void the_exception_is_an_argument_exception()
	        {
	            Assert.IsType<ArgumentException>(_caught.InnerException);
	        }
	    }

	    [Trait("Category", "LongRunning")]
	    public class connect_to_existing_persistent_all_subscription_with_permissions : SpecificationWithMiniNode
	    {
	        private EventStorePersistentSubscription _sub;
	        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
	                                                                .DoNotResolveLinkTos()
	                                                                .StartFromCurrent();
	        protected override void When()
	        {
	            _conn.CreatePersistentSubscriptionForAllAsync("agroupname17", _settings, DefaultData.AdminCredentials).Wait();
	            _sub = _conn.ConnectToPersistentSubscriptionForAll("agroupname17",
	                (sub, e) => Console.Write("appeared"),
	                (sub, reason, ex) => { }, DefaultData.AdminCredentials);
	        }

	        [Fact]
	        public void the_subscription_suceeds()
	        {
	            Assert.NotNull(_sub);
	        }
	    }

	    [Trait("Category", "LongRunning")]
	    public class connect_to_existing_persistent_all_subscription_without_permissions : SpecificationWithMiniNode
	    {
	        private readonly PersistentSubscriptionSettings _settings = PersistentSubscriptionSettingsBuilder.Create()
	                                                                .DoNotResolveLinkTos()
	                                                                .StartFromCurrent();

	        protected override void When()
	        {
	            _conn.CreatePersistentSubscriptionForAllAsync("agroupname55", _settings,
	                DefaultData.AdminCredentials).Wait();
	        }

	        [Fact]
	        public void the_subscription_fails_to_connect()
	        {
	            try
	            {
	                _conn.ConnectToPersistentSubscriptionForAll("agroupname55",
	                    (sub, e) => Console.Write("appeared"),
	                    (sub, reason, ex) => { });
	                throw new Exception("should have thrown.");
	            }
	            catch (Exception ex)
	            {
	                Assert.IsType<AggregateException>(ex);
	                Assert.IsType<AccessDeniedException>(ex.InnerException);
	            }
	        }
	    }
	*/
}
