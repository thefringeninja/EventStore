using System;
using System.Linq;
using System.Threading;
using EventStore.Core.Bus;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Bus {
	[Trait("Category", "LongRunning")]
	public abstract class when_publishing_to_queued_handler : QueuedHandlerTestWithWaitingConsumer {
		protected when_publishing_to_queued_handler(
			Func<IHandle<Message>, string, TimeSpan, IQueuedHandler> queuedHandlerFactory)
			: base(queuedHandlerFactory) {
			Queue.Start();
		}

		public override void Dispose() {
			Consumer?.Dispose();
			Queue?.Stop();
			base.Dispose();
		}

		[Fact(Skip = "We do not check each message for null for performance reasons.")]
		public void null_message_should_throw() {
			Assert.Throws<ArgumentNullException>(() => Queue.Publish(null));
		}

		[Fact]
		public void message_it_should_be_delivered_to_bus() {
			Consumer.SetWaitingCount(1);

			Queue.Publish(new TestMessage());

			Consumer.Wait();
			Assert.True(Consumer.HandledMessages.ContainsSingle<TestMessage>());
		}

		[Fact]
		public void multiple_messages_they_should_be_delivered_to_bus() {
			Consumer.SetWaitingCount(2);

			Queue.Publish(new TestMessage());
			Queue.Publish(new TestMessage2());

			Consumer.Wait();

			Assert.True(Consumer.HandledMessages.ContainsSingle<TestMessage>());
			Assert.True(Consumer.HandledMessages.ContainsSingle<TestMessage2>());
		}

		[Fact]
		public void messages_order_should_remain_the_same() {
			Consumer.SetWaitingCount(6);

			Queue.Publish(new TestMessageWithId(4));
			Queue.Publish(new TestMessageWithId(8));
			Queue.Publish(new TestMessageWithId(15));
			Queue.Publish(new TestMessageWithId(16));
			Queue.Publish(new TestMessageWithId(23));
			Queue.Publish(new TestMessageWithId(42));

			Consumer.Wait();

			var typedMessages = Consumer.HandledMessages.OfType<TestMessageWithId>().ToArray();
			Assert.Equal(6, typedMessages.Length);
			Assert.Equal(4, typedMessages[0].Id);
			Assert.Equal(8, typedMessages[1].Id);
			Assert.Equal(15, typedMessages[2].Id);
			Assert.Equal(16, typedMessages[3].Id);
			Assert.Equal(23, typedMessages[4].Id);
			Assert.Equal(42, typedMessages[5].Id);
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_publishing_to_queued_handler_mres : when_publishing_to_queued_handler {
		public when_publishing_to_queued_handler_mres()
			: base((consumer, name, timeout) => new QueuedHandlerMresWithMpsc(consumer, name, false, null, timeout)) {
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_publishing_to_queued_handler_autoreset : when_publishing_to_queued_handler {
		public when_publishing_to_queued_handler_autoreset()
			: base((consumer, name, timeout) => new QueuedHandlerAutoResetWithMpsc(consumer, name, false, null, timeout)
			) {
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_publishing_to_queued_handler_sleep : when_publishing_to_queued_handler {
		public when_publishing_to_queued_handler_sleep()
			: base((consumer, name, timeout) => new QueuedHandlerSleep(consumer, name, false, null, timeout)) {
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_publishing_to_queued_handler_pulse : when_publishing_to_queued_handler {
		public when_publishing_to_queued_handler_pulse()
			: base((consumer, name, timeout) => new QueuedHandlerPulse(consumer, name, false, null, timeout)) {
		}
	}

	[Trait("Category", "LongRunning")]
	public class when_publishing_to_queued_handler_threadpool : when_publishing_to_queued_handler {
		public when_publishing_to_queued_handler_threadpool()
			: base((consumer, name, timeout) => new QueuedHandlerThreadPool(consumer, name, false, null, timeout)) {
		}
	}
}
