using System;
using System.Collections.Generic;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.RequestManager.Managers;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Replication.TransactionCommit {
	public class when_transaction_commit_gets_prepare_timeout_after_prepares : RequestManagerSpecification {
		protected override TwoPhaseRequestManagerBase OnManager(FakePublisher publisher) {
			return new TransactionCommitTwoPhaseRequestManager(publisher, 3, PrepareTimeout, CommitTimeout, false);
		}

		protected override IEnumerable<Message> WithInitialMessages() {
			yield return new ClientMessage.TransactionCommit(InternalCorrId, ClientCorrId, Envelope, true, 4, null);
			yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.SingleWrite);
			yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.SingleWrite);
			yield return new StorageMessage.PrepareAck(InternalCorrId, 1, PrepareFlags.SingleWrite);
		}

		protected override Message When() {
			return new StorageMessage.RequestManagerTimerTick(
				DateTime.UtcNow + TimeSpan.FromTicks(CommitTimeout.Ticks / 2));
		}

		[Fact]
		public void no_messages_are_published() {
			Assert.True(Produced.Count == 0);
		}

		[Fact]
		public void the_envelope_is_not_replied_to() {
			Assert.Equal(0, Envelope.Replies.Count);
		}
	}
}
