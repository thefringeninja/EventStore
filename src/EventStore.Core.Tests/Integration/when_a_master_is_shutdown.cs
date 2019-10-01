using EventStore.Core.Bus;
using EventStore.Core.Messages;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Integration {
	[Trait("Category", "LongRunning")]
	public class when_a_master_is_shutdown : specification_with_cluster {
		private List<Guid> _epochIds = new List<Guid>();
		private List<string> _roleAssignments = new List<string>();
		private CountdownEvent _expectedNumberOfEvents;
		private object _lock = new object();

		protected override void BeforeNodesStart() {
			_nodes.ToList().ForEach(x => {
				x.Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.BecomeMaster>(Handle));
				x.Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.BecomeSlave>(Handle));
				x.Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.EpochWritten>(Handle));
			});

			_expectedNumberOfEvents = new CountdownEvent(3 /*role assignments*/ + 1 /*epoch write*/);
			base.BeforeNodesStart();
		}

		protected override async Task Given() {
			_expectedNumberOfEvents.Wait(5000);
			var master = _nodes.First(x => x.NodeState == Data.VNodeState.Master);
			await ShutdownNode(master.DebugIndex);
			_expectedNumberOfEvents = new CountdownEvent(2 /*role assignments*/ + 1 /*epoch write*/);
			_expectedNumberOfEvents.Wait(5000);
			await base.Given();
		}

		private void Handle(SystemMessage.BecomeMaster msg) {
			lock (_lock) {
				_roleAssignments.Add("master");
			}

			_expectedNumberOfEvents?.Signal();
		}

		private void Handle(SystemMessage.BecomeSlave msg) {
			lock (_lock) {
				_roleAssignments.Add("slave");
			}

			_expectedNumberOfEvents?.Signal();
		}

		private void Handle(SystemMessage.EpochWritten msg) {
			lock (_lock) {
				_epochIds.Add(msg.Epoch.EpochId);
			}

			_expectedNumberOfEvents?.Signal();
		}

		[Fact(Skip = "Flaky test - e.g. if multiple elections take place")]
		public void should_assign_master_and_slave_roles_correctly() {
			Assert.Equal(5, _roleAssignments.Count());

			Assert.Equal(1, _roleAssignments.Take(3).Where(x => x.Equals("master")).Count());
			Assert.Equal(2, _roleAssignments.Take(3).Where(x => x.Equals("slave")).Count());

			//after shutting down
			Assert.Equal(1, _roleAssignments.Skip(3).Take(2).Where(x => x.Equals("master")).Count());
			Assert.Equal(1, _roleAssignments.Skip(3).Take(2).Where(x => x.Equals("slave")).Count());
		}

		[Fact(Skip = "Flaky test - e.g. if multiple elections take place")]
		public void should_have_two_unique_epoch_writes() {
			Assert.Equal(2, _epochIds.Distinct().Count());
		}
	}
}
