using System;
using EventStore.Core.Bus;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Bus {
	public class when_unsubscribing_from_memory_bus : IDisposable {
		private InMemoryBus _bus;

		public when_unsubscribing_from_memory_bus() {
			_bus = new InMemoryBus("test_bus", watchSlowMsg: false);
		}

		public void Dispose() {
			_bus = null;
		}

		[Fact]
		public void null_as_handler_app_should_throw() {
			Assert.Throws<ArgumentNullException>(() => _bus.Unsubscribe<TestMessage>(null));
		}

		[Fact]
		public void not_subscribed_handler_app_doesnt_throw() {
			var handler = new TestHandler<TestMessage>();
			_bus.Unsubscribe<TestMessage>(handler);
		}

		[Fact]
		public void same_handler_from_same_message_multiple_times_app_doesnt_throw() {
			var handler = new TestHandler<TestMessage>();
			_bus.Unsubscribe<TestMessage>(handler);
			_bus.Unsubscribe<TestMessage>(handler);
			_bus.Unsubscribe<TestMessage>(handler);
		}

		[Fact]
		public void multihandler_from_single_message_app_doesnt_throw() {
			var handler = new TestMultiHandler();
			_bus.Subscribe<TestMessage>(handler);
			_bus.Subscribe<TestMessage2>(handler);
			_bus.Subscribe<TestMessage3>(handler);

			_bus.Unsubscribe<TestMessage>(handler);
		}

		[Fact]
		public void handler_from_message_it_should_not_handle_this_message_anymore() {
			var handler = new TestHandler<TestMessage>();
			_bus.Subscribe<TestMessage>(handler);

			_bus.Unsubscribe<TestMessage>(handler);
			_bus.Publish(new TestMessage());

			Assert.True(handler.HandledMessages.IsEmpty());
		}

		[Fact]
		public void handler_from_multiple_messages_they_all_should_not_be_handled_anymore() {
			var handler = new TestMultiHandler();
			_bus.Subscribe<TestMessage>(handler);
			_bus.Subscribe<TestMessage2>(handler);
			_bus.Subscribe<TestMessage3>(handler);

			_bus.Unsubscribe<TestMessage>(handler);
			_bus.Unsubscribe<TestMessage2>(handler);
			_bus.Unsubscribe<TestMessage3>(handler);

			_bus.Publish(new TestMessage());
			_bus.Publish(new TestMessage2());
			_bus.Publish(new TestMessage3());

			Assert.True(handler.HandledMessages.ContainsNo<TestMessage>() &&
			            handler.HandledMessages.ContainsNo<TestMessage2>() &&
			            handler.HandledMessages.ContainsNo<TestMessage3>());
		}

		[Fact]
		public void handler_from_message_it_should_not_handle_this_message_anymore_and_still_handle_other_messages() {
			var handler = new TestMultiHandler();
			_bus.Subscribe<TestMessage>(handler);
			_bus.Subscribe<TestMessage2>(handler);
			_bus.Subscribe<TestMessage3>(handler);

			_bus.Unsubscribe<TestMessage>(handler);

			_bus.Publish(new TestMessage());
			_bus.Publish(new TestMessage2());
			_bus.Publish(new TestMessage3());

			Assert.True(handler.HandledMessages.ContainsNo<TestMessage>() &&
			            handler.HandledMessages.ContainsSingle<TestMessage2>() &&
			            handler.HandledMessages.ContainsSingle<TestMessage3>());
		}

		[Fact]
		public void one_handler_and_leaving_others_subscribed_only_others_should_handle_message() {
			var handler1 = new TestHandler<TestMessage>();
			var handler2 = new TestHandler<TestMessage>();
			var handler3 = new TestHandler<TestMessage>();

			_bus.Subscribe<TestMessage>(handler1);
			_bus.Subscribe<TestMessage>(handler2);
			_bus.Subscribe<TestMessage>(handler3);

			_bus.Unsubscribe(handler1);
			_bus.Publish(new TestMessage());

			Assert.True(handler1.HandledMessages.ContainsNo<TestMessage>() &&
			            handler2.HandledMessages.ContainsSingle<TestMessage>() &&
			            handler3.HandledMessages.ContainsSingle<TestMessage>());
		}

		[Fact]
		public void all_handlers_from_message_noone_should_handle_message() {
			var handler1 = new TestHandler<TestMessage>();
			var handler2 = new TestHandler<TestMessage>();
			var handler3 = new TestHandler<TestMessage>();

			_bus.Subscribe<TestMessage>(handler1);
			_bus.Subscribe<TestMessage>(handler2);
			_bus.Subscribe<TestMessage>(handler3);

			_bus.Unsubscribe(handler1);
			_bus.Unsubscribe(handler2);
			_bus.Unsubscribe(handler3);
			_bus.Publish(new TestMessage());

			Assert.True(handler1.HandledMessages.ContainsNo<TestMessage>() &&
			            handler2.HandledMessages.ContainsNo<TestMessage>() &&
			            handler3.HandledMessages.ContainsNo<TestMessage>());
		}

		[Fact]
		public void handlers_after_publishing_message_all_is_still_done_correctly() {
			var handler1 = new TestHandler<TestMessage>();
			var handler2 = new TestHandler<TestMessage>();
			var handler3 = new TestHandler<TestMessage>();

			_bus.Subscribe<TestMessage>(handler1);
			_bus.Subscribe<TestMessage>(handler2);
			_bus.Subscribe<TestMessage>(handler3);

			_bus.Publish(new TestMessage());
			handler1.HandledMessages.Clear();
			handler2.HandledMessages.Clear();
			handler3.HandledMessages.Clear();

			//just to ensure
			Assert.True(handler1.HandledMessages.ContainsNo<TestMessage>() &&
			            handler2.HandledMessages.ContainsNo<TestMessage>() &&
			            handler3.HandledMessages.ContainsNo<TestMessage>());

			_bus.Unsubscribe(handler1);
			_bus.Unsubscribe(handler2);
			_bus.Unsubscribe(handler3);
			_bus.Publish(new TestMessage());

			Assert.True(handler1.HandledMessages.ContainsNo<TestMessage>() &&
			            handler2.HandledMessages.ContainsNo<TestMessage>() &&
			            handler3.HandledMessages.ContainsNo<TestMessage>());
		}
	}
}
