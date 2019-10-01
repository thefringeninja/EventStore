using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.parallel_processing_load_balancer {
	public class when_scheduling_first_tasks : specification_with_parallel_processing_load_balancer {
		private List<string> _scheduledTasks;
		private List<int> _scheduledOnWorkers;
		private int _scheduled;

		protected override void Given() {
			_scheduled = 0;
			_scheduledTasks = new List<string>();
			_scheduledOnWorkers = new List<int>();
		}

		protected override void When() {
			_balancer.ScheduleTask("task1", OnScheduled);
			_balancer.ScheduleTask("task2", OnScheduled);
		}

		private void OnScheduled(string task, int worker) {
			_scheduled++;
			_scheduledTasks.Add(task);
			_scheduledOnWorkers.Add(worker);
		}

		[Fact]
		public void schedules_all_tasks() {
			Assert.Equal(2, _scheduled);
		}

		[Fact]
		public void schedules_correct_tasks() {
			Assert.True(new[] {"task1", "task2"}.SequenceEqual(_scheduledTasks));
		}

		[Fact]
		public void schedules_on_different_workers() {
			Assert.True(_scheduledOnWorkers.Distinct().Count() == 2);
		}
	}
}
