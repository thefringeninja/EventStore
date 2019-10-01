using System.Collections.Generic;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.RequestManager.Managers;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Replication.TransactionCommit {
	public class when_transaction_commit_completes_successfully : RequestManagerSpecification {
		protected override TwoPhaseRequestManagerBase OnManager(FakePublisher publisher) {
			return new TransactionCommitTwoPhaseRequestManager(publisher, 3, PrepareTimeout, CommitTimeout, false);
		}

		protected override IEnumerable<Message> WithInitialMessages() {
			yield return new ClientMessage.TransactionCommit(InternalCorrId, ClientCorrId, Envelope, true, 4, null);
			yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.StreamDelete);
			yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.StreamDelete);
			yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.StreamDelete);
		}

		protected override Message When() {
			return new StorageMessage.CommitReplicated(InternalCorrId, 100, 2, 3, 3);
		}

		[Fact]
		public void successful_request_message_is_publised() {
			Assert.True(Produced.ContainsSingle<StorageMessage.RequestCompleted>(
				x => x.CorrelationId == InternalCorrId && x.Success));
		}

		[Fact]
		public void the_envelope_is_replied_to_with_success() {
			Assert.True(Envelope.Replies.ContainsSingle<ClientMessage.TransactionCommitCompleted>(
				x => x.CorrelationId == ClientCorrId && x.Result == OperationResult.Success));
		}
	}
}
