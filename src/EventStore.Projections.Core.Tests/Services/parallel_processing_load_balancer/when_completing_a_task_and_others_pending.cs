using Xunit;

namespace EventStore.Projections.Core.Tests.Services.parallel_processing_load_balancer {
	public class when_completing_a_task_and_others_pending : specification_with_parallel_processing_load_balancer {
		private bool _task5Scheduled;
		private bool _task6Scheduled;

		protected override void Given() {
			_task5Scheduled = false;
			_task6Scheduled = false;

			_balancer.ScheduleTask("task1", (s, i) => { });
			_balancer.ScheduleTask("task2", (s, i) => { });
			_balancer.ScheduleTask("task3", (s, i) => { });
			_balancer.ScheduleTask("task4", (s, i) => { });
			_balancer.AccountMeasured("task1", 1000);
			_balancer.AccountMeasured("task2", 1000);
			_balancer.AccountMeasured("task3", 10);
			_balancer.AccountMeasured("task4", 10);
			_balancer.ScheduleTask("task5", (task, worker) => _task5Scheduled = true);
			_balancer.ScheduleTask("task6", (task, worker) => _task6Scheduled = true);
			Assert.False(_task5Scheduled);
			Assert.False(_task6Scheduled);
		}

		protected override void When() {
			_balancer.AccountCompleted("task2");
		}


		[Fact]
		public void pending_tasks_become_scheduled() {
			Assert.True(_task5Scheduled);
			Assert.True(_task6Scheduled);
		}
	}
}
