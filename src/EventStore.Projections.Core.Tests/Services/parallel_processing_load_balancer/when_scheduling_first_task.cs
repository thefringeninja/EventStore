using Xunit;

namespace EventStore.Projections.Core.Tests.Services.parallel_processing_load_balancer {
	public class when_scheduling_first_task : specification_with_parallel_processing_load_balancer {
		private string _scheduledTask;
		private int _scheduledOnWorker;

		protected override void Given() {
			_scheduledTask = null;
			_scheduledOnWorker = int.MinValue;
		}

		protected override void When() {
			_balancer.ScheduleTask(
				"task1", (task, worker) => {
					_scheduledTask = task;
					_scheduledOnWorker = worker;
				});
		}

		[Fact]
		public void schedules_correct_task() {
			Assert.Equal("task1", _scheduledTask);
		}

		[Fact]
		public void schedules_on_any_worker() {
			Assert.NotEqual(int.MinValue, _scheduledOnWorker);
		}
	}
}
