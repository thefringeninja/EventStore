using System;
using EventStore.Core.Bus;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;
using EventStore.Core.Messaging;

namespace EventStore.Core.Tests.Bus {
	[Trait("Category", "bus")]
	public abstract class when_consumer_throws : QueuedHandlerTestWithWaitingConsumer {
		protected when_consumer_throws(
			Func<IHandle<Message>, string, TimeSpan, IQueuedHandler> queuedHandlerFactory)
			: base(queuedHandlerFactory) {
		}

		public override void Dispose() {
			Consumer.Dispose();
			Queue.Stop();
			base.Dispose();
		}

#if DEBUG
		[Fact(Skip = "This test is not supported with DEBUG conditional since all exceptions are thrown in DEBUG builds.")]
#else
		[Fact]
#endif
		public void all_messages_in_the_queue_should_be_delivered() {
            Consumer.SetWaitingCount(3);

            Queue.Publish(new TestMessage());
            Queue.Publish(new ExecutableTestMessage(() =>
            {
                throw new NullReferenceException();
            }));
            Queue.Publish(new TestMessage2());

            Queue.Start();
            Consumer.Wait();

            Assert.True(Consumer.HandledMessages.ContainsSingle<TestMessage>());
            Assert.True(Consumer.HandledMessages.ContainsSingle<ExecutableTestMessage>());
            Assert.True(Consumer.HandledMessages.ContainsSingle<TestMessage2>());

		}
	}

	[Trait("Category", "bus")]
	public class when_consumer_throws_mres : when_consumer_throws {
		public when_consumer_throws_mres()
			: base((consumer, name, timeout) => new QueuedHandlerMresWithMpsc(consumer, name, false, null, timeout)) {
		}
	}

	[Trait("Category", "bus")]
	public class when_consumer_throws_autoreset : when_consumer_throws {
		public when_consumer_throws_autoreset()
			: base((consumer, name, timeout) =>
				new QueuedHandlerAutoResetWithMpsc(consumer, name, false, null, timeout)) {
		}
	}
}
