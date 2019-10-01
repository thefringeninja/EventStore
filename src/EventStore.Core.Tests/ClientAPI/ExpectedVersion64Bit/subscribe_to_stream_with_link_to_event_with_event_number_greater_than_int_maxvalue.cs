using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Utils;
using EventStore.Core.Services;
using Xunit;
using System;
using System.Threading;
using ResolvedEvent = EventStore.ClientAPI.ResolvedEvent;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.ClientAPI.ExpectedVersion64Bit {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class
		subscribe_to_stream_with_link_to_event_with_event_number_greater_than_int_maxvalue :
			MiniNodeWithExistingRecords {
		private const string StreamName =
			"subscribe_to_stream_with_link_to_event_with_event_number_greater_than_int_maxvalue";

		private const long intMaxValue = (long)int.MaxValue;

		private string _linkedStreamName = "linked-" + StreamName;
		private Guid _event1Id;

		private readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);
		private ResolvedEvent _receivedEvent;

		public override void WriteTestScenario() {
			var event1 = WriteSingleEvent(StreamName, intMaxValue + 1, new string('.', 3000));
			WriteSingleEvent(StreamName, intMaxValue + 2, new string('.', 3000));
			_event1Id = event1.EventId;
		}

		public override async Task Given() {
			_store = BuildConnection(Node);
            await _store.ConnectAsync();

            await _store.SubscribeToStreamAsync(_linkedStreamName, true, HandleEvent);
            await _store.AppendToStreamAsync(_linkedStreamName, ExpectedVersion.NoStream,
				new EventData(Guid.NewGuid(),
					SystemEventTypes.LinkTo, false, Helper.UTF8NoBom.GetBytes(
						string.Format("{0}@{1}", intMaxValue + 1, StreamName)
					), null));
		}

		private Task HandleEvent(EventStoreSubscription sub, ResolvedEvent resolvedEvent) {
			_receivedEvent = resolvedEvent;
			_resetEvent.Set();
			return Task.CompletedTask;
		}

		[Fact]
		public void should_receive_and_resolve_the_linked_event() {
			Assert.True(_resetEvent.WaitOne(TimeSpan.FromSeconds(10)));
			Assert.Equal(intMaxValue + 1, _receivedEvent.Event.EventNumber);
			Assert.Equal(_event1Id, _receivedEvent.Event.EventId);
			Assert.Equal(intMaxValue + 1, _receivedEvent.Event.EventNumber);
		}
	}
}
