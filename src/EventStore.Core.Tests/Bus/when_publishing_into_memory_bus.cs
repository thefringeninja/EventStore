using System;
using EventStore.Core.Bus;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Bus {
	public class when_publishing_into_memory_bus : IDisposable {
		private InMemoryBus _bus;

		public when_publishing_into_memory_bus() {
			_bus = new InMemoryBus("test_bus", watchSlowMsg: false);
		}

		public void Dispose() {
			_bus = null;
		}

		[Fact(Skip = "We do not check each message for null for performance reasons.")]
		public void null_message_app_should_throw() {
			Assert.Throws<ArgumentNullException>(() => _bus.Publish(null));
		}

		[Fact]
		public void unsubscribed_messages_noone_should_handle_it() {
			var handler1 = new TestHandler<TestMessage>();
			var handler2 = new TestHandler<TestMessage2>();
			var handler3 = new TestHandler<TestMessage3>();

			_bus.Publish(new TestMessage());
			_bus.Publish(new TestMessage2());
			_bus.Publish(new TestMessage3());

			Assert.True(handler1.HandledMessages.Count == 0
			            && handler2.HandledMessages.Count == 0
			            && handler3.HandledMessages.Count == 0);
		}

		[Fact]
		public void any_message_no_other_messages_should_be_published() {
			var handler1 = new TestHandler<TestMessage>();
			var handler2 = new TestHandler<TestMessage2>();

			_bus.Subscribe<TestMessage>(handler1);
			_bus.Subscribe<TestMessage2>(handler2);

			_bus.Publish(new TestMessage());

			Assert.True(handler1.HandledMessages.ContainsSingle<TestMessage>() && handler2.HandledMessages.Count == 0);
		}

		[Fact]
		public void same_message_n_times_it_should_be_handled_n_times() {
			var handler = new TestHandler<TestMessageWithId>();
			var message = new TestMessageWithId(11);

			_bus.Subscribe<TestMessageWithId>(handler);

			_bus.Publish(message);
			_bus.Publish(message);
			_bus.Publish(message);

			Assert.True(handler.HandledMessages.ContainsN<TestMessageWithId>(3, mes => mes.Id == 11));
		}

		[Fact]
		public void multiple_messages_of_same_type_they_all_should_be_delivered() {
			var handler = new TestHandler<TestMessageWithId>();
			var message1 = new TestMessageWithId(1);
			var message2 = new TestMessageWithId(2);
			var message3 = new TestMessageWithId(3);

			_bus.Subscribe<TestMessageWithId>(handler);

			_bus.Publish(message1);
			_bus.Publish(message2);
			_bus.Publish(message3);

			Assert.True(handler.HandledMessages.ContainsSingle<TestMessageWithId>(mes => mes.Id == 1));
			Assert.True(handler.HandledMessages.ContainsSingle<TestMessageWithId>(mes => mes.Id == 2));
			Assert.True(handler.HandledMessages.ContainsSingle<TestMessageWithId>(mes => mes.Id == 3));
		}

		[Fact]
		public void message_of_child_type_then_all_subscribed_handlers_of_parent_type_should_handle_message() {
			var parentHandler = new TestHandler<ParentTestMessage>();
			_bus.Subscribe<ParentTestMessage>(parentHandler);

			_bus.Publish(new ChildTestMessage());

			Assert.True(parentHandler.HandledMessages.ContainsSingle<ChildTestMessage>());
		}

		[Fact]
		public void message_of_parent_type_then_no_subscribed_handlers_of_child_type_should_handle_message() {
			var childHandler = new TestHandler<ChildTestMessage>();
			_bus.Subscribe<ChildTestMessage>(childHandler);

			_bus.Publish(new ParentTestMessage());

			Assert.True(childHandler.HandledMessages.ContainsNo<ParentTestMessage>());
		}

		[Fact]
		public void message_of_grand_child_type_then_all_subscribed_handlers_of_base_types_should_handle_message() {
			var parentHandler = new TestHandler<ParentTestMessage>();
			var childHandler = new TestHandler<ChildTestMessage>();

			_bus.Subscribe<ParentTestMessage>(parentHandler);
			_bus.Subscribe<ChildTestMessage>(childHandler);

			_bus.Publish(new GrandChildTestMessage());

			Assert.True(parentHandler.HandledMessages.ContainsSingle<GrandChildTestMessage>() &&
			            childHandler.HandledMessages.ContainsSingle<GrandChildTestMessage>());
		}

		[Fact]
		public void
			message_of_grand_child_type_then_all_subscribed_handlers_of_parent_types_including_grand_child_handler_should_handle_message() {
			var parentHandler = new TestHandler<ParentTestMessage>();
			var childHandler = new TestHandler<ChildTestMessage>();
			var grandChildHandler = new TestHandler<GrandChildTestMessage>();

			_bus.Subscribe<ParentTestMessage>(parentHandler);
			_bus.Subscribe<ChildTestMessage>(childHandler);
			_bus.Subscribe<GrandChildTestMessage>(grandChildHandler);

			_bus.Publish(new GrandChildTestMessage());

			Assert.True(parentHandler.HandledMessages.ContainsSingle<GrandChildTestMessage>() &&
			            childHandler.HandledMessages.ContainsSingle<GrandChildTestMessage>() &&
			            grandChildHandler.HandledMessages.ContainsSingle<GrandChildTestMessage>());
		}
	}
}
