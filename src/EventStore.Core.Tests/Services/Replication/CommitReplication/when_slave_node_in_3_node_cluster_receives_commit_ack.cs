using System;
using System.Threading;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using Xunit;

namespace EventStore.Core.Tests.Services.Replication.CommitReplication {
	public class when_slave_node_in_3_node_cluster_receives_commit_ack : with_index_committer_service {
		private long _logPosition = 1000;
		private CountdownEvent _eventsReplicated = new CountdownEvent(1);

		public override void When() {
			_publisher.Subscribe(new AdHocHandler<StorageMessage.CommitReplicated>(m => _eventsReplicated.Signal()));
			_expectedCommitReplicatedMessages = 1;
			BecomeSlave();
			AddPendingPrepare(_logPosition);
			_service.Handle(new StorageMessage.CommitAck(Guid.NewGuid(), _logPosition, _logPosition, 0, 0));

			if (!_eventsReplicated.Wait(TimeSpan.FromSeconds(_timeoutSeconds))) {
				throw new Exception("Timed out waiting for commit replicated messages to be published");
			}
		}

		[Fact]
		public void replication_checkpoint_should_have_been_updated() {
			Assert.Equal(_logPosition, _replicationCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void commit_replicated_message_should_have_been_published() {
			Assert.Equal(1, _handledMessages.Count);
			Assert.Equal(_logPosition, _handledMessages[0].TransactionPosition);
		}

		[Fact]
		public void index_should_have_been_updated() {
			Assert.Equal(1, _indexCommitter.CommittedPrepares.Count);
			Assert.Equal(_logPosition, _indexCommitter.CommittedPrepares[0].LogPosition);
		}
	}
}
