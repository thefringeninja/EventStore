using System;
using System.Collections;
using System.Collections.Generic;
using EventStore.Core.Bus;
using EventStore.Core.Helpers;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.Bus;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.Tests.Services.TimeService;
using Xunit;
using System.Linq;

namespace EventStore.Core.Tests.Helpers {
	public abstract class TestFixtureWithReadWriteDispatchers : IDisposable {
		protected InMemoryBus _bus;

		protected RequestResponseDispatcher<ClientMessage.DeleteStream, ClientMessage.DeleteStreamCompleted>
			_streamDispatcher;

		protected RequestResponseDispatcher<ClientMessage.WriteEvents, ClientMessage.WriteEventsCompleted>
			_writeDispatcher;

		protected
			RequestResponseDispatcher
			<ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted>
			_readDispatcher;

		public TestHandler<Message> Consumer;
		protected IODispatcher _ioDispatcher;
		public ManualQueue Queue;
		protected ManualQueue[] _otherQueues;
		protected FakeTimeProvider _timeProvider;
		private PublishEnvelope _envelope;

		protected IEnvelope Envelope {
			get {
				if (_envelope == null)
					_envelope = new PublishEnvelope(GetInputQueue());
				return _envelope;
			}
		}

		protected List<Message> HandledMessages {
			get { return Consumer.HandledMessages; }
		}

		public TestFixtureWithReadWriteDispatchers() {
			_envelope = null;
			_timeProvider = new FakeTimeProvider();
			_bus = new InMemoryBus("bus");
			Consumer = new TestHandler<Message>();
			_bus.Subscribe(Consumer);
			Queue = GiveInputQueue();
			_otherQueues = null;
			_ioDispatcher = new IODispatcher(_bus, new PublishEnvelope(GetInputQueue()));
			_readDispatcher = _ioDispatcher.BackwardReader;
			_writeDispatcher = _ioDispatcher.Writer;
			_streamDispatcher = _ioDispatcher.StreamDeleter;

			_bus.Subscribe(_ioDispatcher.ForwardReader);
			_bus.Subscribe(_ioDispatcher.BackwardReader);
			_bus.Subscribe(_ioDispatcher.ForwardReader);
			_bus.Subscribe(_ioDispatcher.Writer);
			_bus.Subscribe(_ioDispatcher.StreamDeleter);
			_bus.Subscribe(_ioDispatcher.Awaker);
			_bus.Subscribe(_ioDispatcher);
		}

		public virtual void Dispose() {
		}

		protected virtual ManualQueue GiveInputQueue() => null;

		protected IPublisher GetInputQueue() {
			return (IPublisher)Queue ?? _bus;
		}

		protected void DisableTimer() {
			Queue.DisableTimer();
		}

		protected void EnableTimer() {
			Queue.EnableTimer();
		}

		protected void WhenLoop() {
			Queue.Process();
			var steps = PreWhen().Concat(When());
			WhenLoop(steps);
		}

		protected void WhenLoop(IEnumerable<WhenStep> steps) {
			foreach (var step in steps) {
				_timeProvider.AddTime(TimeSpan.FromMilliseconds(10));
				step.Action?.Invoke();

				foreach (var message in step) {
					if (message != null)
						Queue.Publish(message);
				}

				Queue.ProcessTimer();
				if (_otherQueues != null)
					foreach (var other in _otherQueues)
						other.ProcessTimer();

				var count = 1;
				var total = 0;
				while (count > 0) {
					count = 0;
					count += Queue.ProcessNonTimer();
					if (_otherQueues != null)
						foreach (var other in _otherQueues)
							count += other.ProcessNonTimer();
					total += count;
					if (total > 2000)
						throw new Exception("Infinite loop?");
				}

				// process final timer messages
			}

			Queue.Process();
			if (_otherQueues != null)
				foreach (var other in _otherQueues)
					other.Process();
		}

		public static T EatException<T>(Func<T> func, T defaultValue = default(T)) {
			try {
				return func();
			} catch (Exception) {
				return defaultValue;
			}
		}

		public sealed class WhenStep : IEnumerable<Message> {
			public readonly Action Action;
			public readonly Message Message;
			public readonly IEnumerable<Message> Messages;

			private WhenStep(Message message) {
				Message = message;
			}

			internal WhenStep(IEnumerable<Message> messages) {
				Messages = messages;
			}

			public WhenStep(params Message[] messages) {
				Messages = messages;
			}

			public WhenStep(Action action) {
				Action = action;
			}

			internal WhenStep() {
			}

			public static implicit operator WhenStep(Message message) {
				return new WhenStep(message);
			}

			public IEnumerator<Message> GetEnumerator() {
				return GetMessages().GetEnumerator();
			}

			private IEnumerable<Message> GetMessages() {
				if (Message != null)
					yield return Message;
				else if (Messages != null)
					foreach (var message in Messages)
						yield return message;
				else yield return null;
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}
		}

		protected virtual IEnumerable<WhenStep> PreWhen() {
			yield break;
		}

		protected virtual IEnumerable<WhenStep> When() {
			yield break;
		}

		public readonly WhenStep Yield = new WhenStep();
	}

	public static class TestUtils {
		public static TestFixtureWithReadWriteDispatchers.WhenStep ToSteps(
			this IEnumerable<TestFixtureWithReadWriteDispatchers.WhenStep> steps) {
			return new TestFixtureWithReadWriteDispatchers.WhenStep(steps.SelectMany(v => v));
		}
	}
}
