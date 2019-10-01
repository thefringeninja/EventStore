using System;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messaging;
using Xunit;

namespace EventStore.Core.Tests.Bus.Helpers {
	public abstract class QueuedHandlerTestWithWaitingConsumer : IDisposable {
		private readonly Func<IHandle<Message>, string, TimeSpan, IQueuedHandler> _queuedHandlerFactory;

		protected IQueuedHandler Queue;
		protected WaitingConsumer Consumer;

		protected QueuedHandlerTestWithWaitingConsumer(
			Func<IHandle<Message>, string, TimeSpan, IQueuedHandler> queuedHandlerFactory) {
			Ensure.NotNull(queuedHandlerFactory, "queuedHandlerFactory");
			_queuedHandlerFactory = queuedHandlerFactory;
			Consumer = new WaitingConsumer(0);
			Queue = _queuedHandlerFactory(Consumer, "waiting_queue", TimeSpan.FromMilliseconds(5000));
		}

		public virtual void Dispose() {
			Queue?.Stop();
			Queue = null;
			Consumer?.Dispose();
			Consumer = null;
		}
	}
}
