using System;
using System.Threading;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using Xunit;

namespace EventStore.Core.Tests.Services.Replication.CommitReplication {
	public class when_3_node_cluster_receives_multiple_acks_for_same_positions : with_index_committer_service {
		private CountdownEvent _eventsReplicated = new CountdownEvent(2);

		private Guid _correlationId1 = Guid.NewGuid();
		private Guid _correlationId2 = Guid.NewGuid();

		private long _logPosition = 1000;

		public override void When() {
			_publisher.Subscribe(new AdHocHandler<StorageMessage.CommitReplicated>(m => _eventsReplicated.Signal()));
			BecomeMaster();
			AddPendingPrepare(_logPosition);
			_service.Handle(new StorageMessage.CommitAck(_correlationId1, _logPosition, _logPosition, 0, 0, true));
			_service.Handle(new StorageMessage.CommitAck(_correlationId2, _logPosition, _logPosition, 0, 0, true));
			_service.Handle(new StorageMessage.CommitAck(_correlationId2, _logPosition, _logPosition, 0, 0));

			if (!_eventsReplicated.Wait(TimeSpan.FromSeconds(_timeoutSeconds))) {
				throw new Exception("Timed out waiting for commit replicated messages to be published");
			}
		}

		[Fact]
		public void replication_checkpoint_should_have_been_updated() {
			Assert.Equal(_logPosition, _replicationCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void commit_replicated_message_should_have_been_published_for_the_event() {
			Assert.Equal(2, _handledMessages.Count);
		}

		[Fact]
		public void index_should_have_been_updated() {
			Assert.Equal(1, _indexCommitter.CommittedPrepares.Count);
			Assert.Equal(_logPosition, _indexCommitter.CommittedPrepares[0].LogPosition);
		}
	}
}
