using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.RequestManager.Managers;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Replication.DeleteStream {
	public class when_delete_stream_gets_commit_timeout_before_commit_stage : RequestManagerSpecification {
		protected override TwoPhaseRequestManagerBase OnManager(FakePublisher publisher) {
			return new DeleteStreamTwoPhaseRequestManager(publisher, 3, PrepareTimeout, CommitTimeout, false);
		}

		protected override IEnumerable<Message> WithInitialMessages() {
			yield return new ClientMessage.DeleteStream(InternalCorrId, ClientCorrId, Envelope, true, "test123",
				ExpectedVersion.Any, true, null);
			yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.SingleWrite);
			yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.SingleWrite);
		}

		protected override Message When() {
			return new StorageMessage.RequestManagerTimerTick();
		}

		[Fact(Skip = "There is no specialized prepare/commit timeout messages, so not possible to test this.")]
		public void no_messages_are_published() {
			Assert.Equal(0, Produced.Count);
		}

		[Fact(Skip = "There is no specialized prepare/commit timeout messages, so not possible to test this.")]
		public void the_envelope_is_not_replied_to() {
			Assert.Equal(0, Envelope.Replies.Count);
		}
	}
}
