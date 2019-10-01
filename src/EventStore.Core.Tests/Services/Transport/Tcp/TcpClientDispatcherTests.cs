using EventStore.Core.Bus;
using EventStore.Core.Services.Transport.Tcp;
using Xunit;
using System;
using EventStore.Core.Authentication;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.Authentication;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using System.Text;
using EventStore.Core.Services.UserManagement;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Core.Services;
using EventStore.Core.Util;

namespace EventStore.Core.Tests.Services.Transport.Tcp {
	public class TcpClientDispatcherTests {
		private readonly NoopEnvelope _envelope = new NoopEnvelope();
		private const byte _version = (byte)ClientVersion.V1;

		private ClientTcpDispatcher _dispatcher;
		private TcpConnectionManager _connection;

		public TcpClientDispatcherTests() {
			_dispatcher = new ClientTcpDispatcher();

			var dummyConnection = new DummyTcpConnection();
			_connection = new TcpConnectionManager(
				Guid.NewGuid().ToString(), TcpServiceType.External, new ClientTcpDispatcher(),
				InMemoryBus.CreateTest(), dummyConnection, InMemoryBus.CreateTest(), new InternalAuthenticationProvider(
					new Core.Helpers.IODispatcher(InMemoryBus.CreateTest(), new NoopEnvelope()),
					new StubPasswordHashAlgorithm(), 1),
				TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10), (man, err) => { },
				Opts.ConnectionPendingSendBytesThresholdDefault, Opts.ConnectionQueueSizeThresholdDefault);
		}

		[Fact]
		public void when_unwrapping_message_that_does_not_have_version1_unwrapper_should_use_version2_unwrapper() {
			var dto = new TcpClientMessageDto.DeleteStream("test-stream", ExpectedVersion.Any, true, false);
			var package = new TcpPackage(TcpCommand.DeleteStream, Guid.NewGuid(), dto.Serialize());

			var msg = _dispatcher.UnwrapPackage(package, _envelope, SystemAccount.Principal, "", "", _connection,
				_version) as ClientMessage.DeleteStream;
			Assert.NotNull(msg);
		}

		[Fact]
		public void when_wrapping_message_that_does_not_have_version1_wrapper_should_use_version2_wrapper() {
			var msg = new ClientMessage.DeleteStream(Guid.NewGuid(), Guid.NewGuid(), _envelope, true, "test-stream",
				ExpectedVersion.Any, false, SystemAccount.Principal);
			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.DeleteStream, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.DeleteStream>();
			Assert.NotNull(dto);
		}

		[Fact]
		public void
			when_wrapping_read_stream_events_forward_and_stream_was_deleted_should_downgrade_last_event_number() {
			var msg = new ClientMessage.ReadStreamEventsForwardCompleted(Guid.NewGuid(), "test-stream", 0, 100,
				ReadStreamResult.StreamDeleted, new ResolvedEvent[0], new StreamMetadata(),
				true, "", -1, long.MaxValue, true, 1000);

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadStreamEventsForwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadStreamEventsCompleted>();
			Assert.NotNull(dto);

			Assert.Equal(int.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_stream_events_forward_and_stream_was_deleted_should_not_downgrade_last_event_number_for_v2_clients() {
			var msg = new ClientMessage.ReadStreamEventsForwardCompleted(Guid.NewGuid(), "test-stream", 0, 100,
				ReadStreamResult.StreamDeleted, new ResolvedEvent[0], new StreamMetadata(),
				true, "", -1, long.MaxValue, true, 1000);

			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadStreamEventsForwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadStreamEventsCompleted>();
			Assert.NotNull(dto);

			Assert.Equal(long.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_stream_events_backward_and_stream_was_deleted_should_downgrade_last_event_number() {
			var msg = new ClientMessage.ReadStreamEventsBackwardCompleted(Guid.NewGuid(), "test-stream", 0, 100,
				ReadStreamResult.StreamDeleted, new ResolvedEvent[0], new StreamMetadata(),
				true, "", -1, long.MaxValue, true, 1000);

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadStreamEventsBackwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadStreamEventsCompleted>();
			Assert.NotNull(dto);

			Assert.Equal(int.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_stream_events_backward_and_stream_was_deleted_should_not_downgrade_last_event_number_for_v2_clients() {
			var msg = new ClientMessage.ReadStreamEventsBackwardCompleted(Guid.NewGuid(), "test-stream", 0, 100,
				ReadStreamResult.StreamDeleted, new ResolvedEvent[0], new StreamMetadata(),
				true, "", -1, long.MaxValue, true, 1000);

			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadStreamEventsBackwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadStreamEventsCompleted>();
			Assert.NotNull(dto);

			Assert.Equal(long.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void when_wrapping_read_all_events_forward_completed_with_deleted_event_should_downgrade_version() {
			var events = new ResolvedEvent[] {
				ResolvedEvent.ForUnresolvedEvent(CreateDeletedEventRecord(), 0),
			};
			var msg = new ClientMessage.ReadAllEventsForwardCompleted(Guid.NewGuid(), ReadAllResult.Success, "", events,
				new StreamMetadata(), true, 10, new TFPos(0, 0),
				new TFPos(200, 200), new TFPos(0, 0), 100);

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadAllEventsForwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadAllEventsCompleted>();
			Assert.NotNull(dto);
			Assert.Equal(1, dto.Events.Count());

			Assert.Equal(int.MaxValue, dto.Events[0].Event.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_all_events_forward_completed_with_deleted_event_should_not_downgrade_last_event_number_for_v2_clients() {
			var events = new ResolvedEvent[] {
				ResolvedEvent.ForUnresolvedEvent(CreateDeletedEventRecord(), 0),
			};
			var msg = new ClientMessage.ReadAllEventsForwardCompleted(Guid.NewGuid(), ReadAllResult.Success, "", events,
				new StreamMetadata(), true, 10, new TFPos(0, 0),
				new TFPos(200, 200), new TFPos(0, 0), 100);

			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadAllEventsForwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadAllEventsCompleted>();
			Assert.NotNull(dto);
			Assert.Equal(1, dto.Events.Count());

			Assert.Equal(long.MaxValue, dto.Events[0].Event.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_all_events_forward_completed_with_link_to_deleted_event_should_downgrade_version() {
			var events = new ResolvedEvent[] {
				ResolvedEvent.ForResolvedLink(CreateLinkEventRecord(), CreateDeletedEventRecord(), 100)
			};
			var msg = new ClientMessage.ReadAllEventsForwardCompleted(Guid.NewGuid(), ReadAllResult.Success, "", events,
				new StreamMetadata(), true, 10, new TFPos(0, 0),
				new TFPos(200, 200), new TFPos(0, 0), 100);

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadAllEventsForwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadAllEventsCompleted>();
			Assert.NotNull(dto);
			Assert.Equal(1, dto.Events.Count());

			Assert.Equal(0, dto.Events[0].Event.EventNumber);
			Assert.Equal(int.MaxValue, dto.Events[0].Link.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_all_events_forward_completed_with_link_to_deleted_event_should_not_downgrade_version_for_v2_clients() {
			var events = new ResolvedEvent[] {
				ResolvedEvent.ForResolvedLink(CreateLinkEventRecord(), CreateDeletedEventRecord(), 100)
			};
			var msg = new ClientMessage.ReadAllEventsForwardCompleted(Guid.NewGuid(), ReadAllResult.Success, "", events,
				new StreamMetadata(), true, 10, new TFPos(0, 0),
				new TFPos(200, 200), new TFPos(0, 0), 100);

			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadAllEventsForwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadAllEventsCompleted>();
			Assert.NotNull(dto);
			Assert.Equal(1, dto.Events.Count());

			Assert.Equal(0, dto.Events[0].Event.EventNumber);
			Assert.Equal(long.MaxValue, dto.Events[0].Link.EventNumber);
		}

		[Fact]
		public void when_wrapping_read_all_events_backward_completed_with_deleted_event_should_downgrade_version() {
			var events = new ResolvedEvent[] {
				ResolvedEvent.ForUnresolvedEvent(CreateDeletedEventRecord(), 0),
			};
			var msg = new ClientMessage.ReadAllEventsBackwardCompleted(Guid.NewGuid(), ReadAllResult.Success, "",
				events,
				new StreamMetadata(), true, 10, new TFPos(0, 0),
				new TFPos(200, 200), new TFPos(0, 0), 100);

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadAllEventsBackwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadAllEventsCompleted>();
			Assert.NotNull(dto);
			Assert.Equal(1, dto.Events.Count());

			Assert.Equal(int.MaxValue, dto.Events[0].Event.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_all_events_backward_completed_with_deleted_event_should_not_downgrade_version_for_v2_clients() {
			var events = new ResolvedEvent[] {
				ResolvedEvent.ForUnresolvedEvent(CreateDeletedEventRecord(), 0),
			};
			var msg = new ClientMessage.ReadAllEventsBackwardCompleted(Guid.NewGuid(), ReadAllResult.Success, "",
				events,
				new StreamMetadata(), true, 10, new TFPos(0, 0),
				new TFPos(200, 200), new TFPos(0, 0), 100);

			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadAllEventsBackwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadAllEventsCompleted>();
			Assert.NotNull(dto);
			Assert.Equal(1, dto.Events.Count());

			Assert.Equal(long.MaxValue, dto.Events[0].Event.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_all_events_backward_completed_with_link_to_deleted_event_should_downgrade_version() {
			var events = new ResolvedEvent[] {
				ResolvedEvent.ForResolvedLink(CreateLinkEventRecord(), CreateDeletedEventRecord(), 100)
			};
			var msg = new ClientMessage.ReadAllEventsBackwardCompleted(Guid.NewGuid(), ReadAllResult.Success, "",
				events,
				new StreamMetadata(), true, 10, new TFPos(0, 0),
				new TFPos(200, 200), new TFPos(0, 0), 100);

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadAllEventsBackwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadAllEventsCompleted>();
			Assert.NotNull(dto);
			Assert.Equal(1, dto.Events.Count());

			Assert.Equal(0, dto.Events[0].Event.EventNumber);
			Assert.Equal(int.MaxValue, dto.Events[0].Link.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_read_all_events_backward_completed_with_link_to_deleted_event_should_not_downgrade_version_for_v2_clients() {
			var events = new ResolvedEvent[] {
				ResolvedEvent.ForResolvedLink(CreateLinkEventRecord(), CreateDeletedEventRecord(), 100)
			};
			var msg = new ClientMessage.ReadAllEventsBackwardCompleted(Guid.NewGuid(), ReadAllResult.Success, "",
				events,
				new StreamMetadata(), true, 10, new TFPos(0, 0),
				new TFPos(200, 200), new TFPos(0, 0), 100);

			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.ReadAllEventsBackwardCompleted, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.ReadAllEventsCompleted>();
			Assert.NotNull(dto);
			Assert.Equal(1, dto.Events.Count());

			Assert.Equal(0, dto.Events[0].Event.EventNumber);
			Assert.Equal(long.MaxValue, dto.Events[0].Link.EventNumber);
		}

		[Fact]
		public void when_wrapping_stream_event_appeared_with_deleted_event_should_downgrade_version() {
			var msg = new ClientMessage.StreamEventAppeared(Guid.NewGuid(),
				ResolvedEvent.ForUnresolvedEvent(CreateDeletedEventRecord(), 0));

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.StreamEventAppeared, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.StreamEventAppeared>();
			Assert.NotNull(dto);
			Assert.Equal(int.MaxValue, dto.Event.Event.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_stream_event_appeared_with_deleted_event_should_not_downgrade_version_for_v2_clients() {
			var msg = new ClientMessage.StreamEventAppeared(Guid.NewGuid(),
				ResolvedEvent.ForUnresolvedEvent(CreateDeletedEventRecord(), 0));

			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.StreamEventAppeared, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.StreamEventAppeared>();
			Assert.NotNull(dto);
			Assert.Equal(long.MaxValue, dto.Event.Event.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_subscribe_to_stream_confirmation_when_stream_deleted_should_downgrade_last_event_number() {
			var msg = new ClientMessage.SubscriptionConfirmation(Guid.NewGuid(), 100, long.MaxValue);
			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.SubscriptionConfirmation, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.SubscriptionConfirmation>();
			Assert.NotNull(dto);
			Assert.Equal(int.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void
			when_wrapping_subscribe_to_stream_confirmation_when_stream_deleted_should_not_downgrade_version_for_v2_clients() {
			var msg = new ClientMessage.SubscriptionConfirmation(Guid.NewGuid(), 100, long.MaxValue);
			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.SubscriptionConfirmation, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.SubscriptionConfirmation>();
			Assert.NotNull(dto);
			Assert.Equal(long.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void
			when_wrapping_subscribe_to_stream_confirmation_when_stream_deleted_should_not_downgrade_last_event_number_for_v2_clients() {
			var msg = new ClientMessage.SubscriptionConfirmation(Guid.NewGuid(), 100, long.MaxValue);
			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.SubscriptionConfirmation, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.SubscriptionConfirmation>();
			Assert.NotNull(dto);
			Assert.Equal(long.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void when_wrapping_subscribe_to_stream_confirmation_with_null_last_event_number_should_not_change() {
			var msg = new ClientMessage.SubscriptionConfirmation(Guid.NewGuid(), 100, null);
			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.SubscriptionConfirmation, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.SubscriptionConfirmation>();
			Assert.NotNull(dto);
			Assert.Null(dto.LastEventNumber);
		}

		[Fact]
		public void when_wrapping_stream_event_appeared_with_link_to_deleted_event_should_downgrade_version() {
			var msg = new ClientMessage.StreamEventAppeared(Guid.NewGuid(),
				ResolvedEvent.ForResolvedLink(CreateLinkEventRecord(), CreateDeletedEventRecord(), 0));

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.StreamEventAppeared, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.StreamEventAppeared>();
			Assert.NotNull(dto);
			Assert.Equal(0, dto.Event.Event.EventNumber);
			Assert.Equal(int.MaxValue, dto.Event.Link.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_stream_event_appeared_with_link_to_deleted_event_should_not_downgrade_version_for_v2_clients() {
			var msg = new ClientMessage.StreamEventAppeared(Guid.NewGuid(),
				ResolvedEvent.ForResolvedLink(CreateLinkEventRecord(), CreateDeletedEventRecord(), 0));

			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.StreamEventAppeared, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.StreamEventAppeared>();
			Assert.NotNull(dto);
			Assert.Equal(0, dto.Event.Event.EventNumber);
			Assert.Equal(long.MaxValue, dto.Event.Link.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_persistent_subscription_confirmation_when_stream_deleted_should_downgrade_last_event_number() {
			var msg = new ClientMessage.PersistentSubscriptionConfirmation("subscription", Guid.NewGuid(), 100,
				long.MaxValue);
			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.PersistentSubscriptionConfirmation, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.PersistentSubscriptionConfirmation>();
			Assert.NotNull(dto);
			Assert.Equal(int.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void
			when_wrapping_persistent_subscription_confirmation_when_stream_deleted_should_not_downgrade_last_event_number_for_v2_clients() {
			var msg = new ClientMessage.PersistentSubscriptionConfirmation("subscription", Guid.NewGuid(), 100,
				long.MaxValue);
			var package = _dispatcher.WrapMessage(msg, (byte)ClientVersion.V2);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.PersistentSubscriptionConfirmation, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.PersistentSubscriptionConfirmation>();
			Assert.NotNull(dto);
			Assert.Equal(long.MaxValue, dto.LastEventNumber);
		}

		[Fact]
		public void when_wrapping_persistent_subscription_confirmation_with_null_last_event_number_should_not_change() {
			var msg = new ClientMessage.PersistentSubscriptionConfirmation("subscription", Guid.NewGuid(), 100, null);
			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.PersistentSubscriptionConfirmation, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.PersistentSubscriptionConfirmation>();
			Assert.NotNull(dto);
			Assert.Null(dto.LastEventNumber);
		}

		[Fact]
		public void
			when_wrapping_persistent_subscription_stream_event_appeared_with_deleted_event_should_downgrade_version() {
			var msg = new ClientMessage.PersistentSubscriptionStreamEventAppeared(Guid.NewGuid(),
				ResolvedEvent.ForUnresolvedEvent(CreateDeletedEventRecord(), 0), 0);

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.PersistentSubscriptionStreamEventAppeared, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.PersistentSubscriptionStreamEventAppeared>();
			Assert.NotNull(dto);
			Assert.Equal(int.MaxValue, dto.Event.Event.EventNumber);
		}

		[Fact]
		public void
			when_wrapping_persistent_subscription_stream_event_appeared_with_link_to_deleted_event_should_downgrade_version() {
			var msg = new ClientMessage.PersistentSubscriptionStreamEventAppeared(Guid.NewGuid(),
				ResolvedEvent.ForResolvedLink(CreateLinkEventRecord(), CreateDeletedEventRecord(), 0), 0);

			var package = _dispatcher.WrapMessage(msg, _version);
			Assert.NotNull(package);
			Assert.Equal(TcpCommand.PersistentSubscriptionStreamEventAppeared, package.Value.Command);

			var dto = package.Value.Data.Deserialize<TcpClientMessageDto.PersistentSubscriptionStreamEventAppeared>();
			Assert.NotNull(dto);
			Assert.Equal(0, dto.Event.Event.EventNumber);
			Assert.Equal(int.MaxValue, dto.Event.Link.EventNumber);
		}


		private EventRecord CreateDeletedEventRecord() {
			return new EventRecord(long.MaxValue,
				LogRecord.DeleteTombstone(0, Guid.NewGuid(), Guid.NewGuid(), "test-stream", long.MaxValue));
		}

		private EventRecord CreateLinkEventRecord() {
			return new EventRecord(0, LogRecord.Prepare(100, Guid.NewGuid(), Guid.NewGuid(), 0, 0,
				"link-stream", -1, PrepareFlags.SingleWrite | PrepareFlags.Data, SystemEventTypes.LinkTo,
				Encoding.UTF8.GetBytes(string.Format("{0}@test-stream", long.MaxValue)), new byte[0]));
		}
	}
}
