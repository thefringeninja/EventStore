using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.RequestManager.Managers;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.Services.Replication.WriteStream {
	public class when_write_stream_gets_already_committed : RequestManagerSpecification {
		protected override TwoPhaseRequestManagerBase OnManager(FakePublisher publisher) {
			return new WriteStreamTwoPhaseRequestManager(publisher, 3, PrepareTimeout, CommitTimeout, false);
		}

		protected override IEnumerable<Message> WithInitialMessages() {
			yield return new ClientMessage.WriteEvents(InternalCorrId, ClientCorrId, Envelope, true, "test123",
				ExpectedVersion.Any, new[] {DummyEvent()}, null);
		}

		protected override Message When() {
			return new StorageMessage.AlreadyCommitted(InternalCorrId, "test123", 0, 1);
		}

		[Fact]
		public void successful_request_message_is_publised() {
			Assert.True(Produced.ContainsSingle<StorageMessage.RequestCompleted>(
				x => x.CorrelationId == InternalCorrId && x.Success));
		}

		[Fact]
		public void the_envelope_is_replied_to_with_success() {
			Assert.True(Envelope.Replies.ContainsSingle<ClientMessage.WriteEventsCompleted>(
				x => x.CorrelationId == ClientCorrId && x.Result == OperationResult.Success));
		}
	}
}
