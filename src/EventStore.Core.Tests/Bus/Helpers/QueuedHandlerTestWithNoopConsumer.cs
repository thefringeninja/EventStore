using System;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messaging;
using Xunit;

namespace EventStore.Core.Tests.Bus.Helpers {
	public abstract class QueuedHandlerTestWithNoopConsumer : IDisposable {
		private readonly Func<IHandle<Message>, string, TimeSpan, IQueuedHandler> _queuedHandlerFactory;

		protected IQueuedHandler Queue;
		protected IHandle<Message> Consumer;

		protected QueuedHandlerTestWithNoopConsumer(
			Func<IHandle<Message>, string, TimeSpan, IQueuedHandler> queuedHandlerFactory) {
			Ensure.NotNull(queuedHandlerFactory, "queuedHandlerFactory");
			_queuedHandlerFactory = queuedHandlerFactory;
			Consumer = new NoopConsumer();
			Queue = _queuedHandlerFactory(Consumer, "test_name", TimeSpan.FromMilliseconds(5000));
		}

		public virtual void Dispose() {
			Queue?.Stop();
			Queue = null;
			Consumer = null;
		}
	}
}
