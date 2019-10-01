using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Bus;
using EventStore.Core.Tests.Helpers;
using Xunit;
using EventStore.Core.Tests.Integration;
using EventStore.Core.Messages;
using EventStore.Core.Data;

namespace EventStore.Core.Tests.Replication.ReadStream {
	[Trait("Category", "LongRunning")]
	public class when_reading_an_event_from_a_single_node : specification_with_cluster {
		private CountdownEvent _expectedNumberOfRoleAssignments;
		private string _streamId = "test-stream";
		private long _commitPosition;

		private MiniClusterNode _liveNode;

		protected override void BeforeNodesStart() {
			_nodes.ToList().ForEach(x =>
				x.Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.StateChangeMessage>(Handle)));
			_expectedNumberOfRoleAssignments = new CountdownEvent(3);
			base.BeforeNodesStart();
		}

		private void Handle(SystemMessage.StateChangeMessage msg) {
			switch (msg.State) {
				case Data.VNodeState.Master:
					_expectedNumberOfRoleAssignments.Signal();
					break;
				case Data.VNodeState.Slave:
					_expectedNumberOfRoleAssignments.Signal();
					break;
			}
		}

		protected override async Task Given() {
			_expectedNumberOfRoleAssignments.Wait(5000);

			_liveNode = GetMaster();
			Assert.NotNull(_liveNode);

			var events = new Event[] {new Event(Guid.NewGuid(), "test-type", false, new byte[10], new byte[0])};
			var writeResult = await ReplicationTestHelper.WriteEvent(_liveNode, events, _streamId);
			Assert.Equal(OperationResult.Success, writeResult.Result);
			_commitPosition = writeResult.CommitPosition;

			var slaves = GetSlaves();
			await Task.WhenAll(slaves.Select(s => ShutdownNode(s.DebugIndex)));

			await base.Given();
		}

		[Fact]
		public async Task should_be_able_to_read_event_from_all_forward() {
			var readResult = await ReplicationTestHelper.ReadAllEventsForward(_liveNode, _commitPosition);
			Assert.Single(readResult.Events.Where(x => x.OriginalStreamId == _streamId));
		}

		[Fact]
		public async Task should_be_able_to_read_event_from_all_backward() {
			var readResult = await ReplicationTestHelper.ReadAllEventsBackward(_liveNode, _commitPosition);
			Assert.Single(readResult.Events.Where(x => x.OriginalStreamId == _streamId));
		}

		[Fact]
		public async Task should_not_be_able_to_read_event_from_stream_forward() {
			var readResult = await ReplicationTestHelper.ReadStreamEventsForward(_liveNode, _streamId);
			Assert.Single(readResult.Events);
			Assert.Equal(ReadStreamResult.Success, readResult.Result);
		}

		[Fact]
		public async Task should_not_be_able_to_read_event_from_stream_backward() {
			var readResult =  await ReplicationTestHelper.ReadStreamEventsBackward(_liveNode, _streamId);
			Assert.Single(readResult.Events);
			Assert.Equal(ReadStreamResult.Success, readResult.Result);
		}

		[Fact]
		public async Task should_not_be_able_to_read_event() {
			var readResult = await ReplicationTestHelper.ReadEvent(_liveNode, _streamId, 0);
			Assert.Equal(ReadEventResult.Success, readResult.Result);
		}
	}
}
