using System;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services {
	public class staged_processing_queue {
		public class when_creating {
			private StagedProcessingQueue _q;

			public when_creating() {
				_q = new StagedProcessingQueue(new[] {true});
			}

			[Fact]
			public void it_can_be_created() {
				Assert.NotNull(_q);
			}

			[Fact]
			public void task_can_be_enqueued() {
				_q.Enqueue(new TestTask(1, 1));
			}

			[Fact]
			public void process_does_not_proces_anything() {
				var processed = _q.Process();
				Assert.False(processed);
			}
		}

		public class when_enqueuing_a_one_step_task {
			private StagedProcessingQueue _q;
			private TestTask _t1;

			public when_enqueuing_a_one_step_task() {
				_q = new StagedProcessingQueue(new[] {true});
				_t1 = new TestTask(1, 1);
				_q.Enqueue(_t1);
			}

			[Fact]
			public void process_executes_the_task_at_stage_zero() {
				_q.Process();

				Assert.True(_t1.StartedOn(0));
			}

			[Fact]
			public void process_returns_true() {
				var processed = _q.Process();

				Assert.True(processed);
			}
		}

		public class when_enqueuing_a_two_step_task_and_the_first_step_completes_immediately {
			private StagedProcessingQueue _q;
			private TestTask _t1;

			public when_enqueuing_a_two_step_task_and_the_first_step_completes_immediately() {
				_q = new StagedProcessingQueue(new[] {true, true});
				_t1 = new TestTask(1, 2, 0);
				_q.Enqueue(_t1);
			}

			[Fact]
			public void process_executes_the_task_up_to_stage_one() {
				_q.Process(max: 2);

				Assert.True(_t1.StartedOn(1));
			}
		}

		public class when_reinitializing {
			private StagedProcessingQueue _q;
			private TestTask _t1;

			public when_reinitializing() {
				_q = new StagedProcessingQueue(new[] {true, true});
				_t1 = new TestTask(1, 2, 0);
				_q.Enqueue(_t1);
				_q.Initialize();
			}

			[Fact]
			public void process_does_not_execute_a_task() {
				_q.Process();

				Assert.True(!_t1.StartedOn(0));
			}

			[Fact]
			public void queue_length_iz_zero() {
				Assert.Equal(0, _q.Count);
			}

			[Fact]
			public void task_can_be_enqueued() {
				_q.Enqueue(new TestTask(1, 1));
			}
		}

		public class when_enqueuing_a_two_step_task_and_both_steps_complete_immediately {
			private StagedProcessingQueue _q;
			private TestTask _t1;

			public when_enqueuing_a_two_step_task_and_both_steps_complete_immediately() {
				_q = new StagedProcessingQueue(new[] {true, true});
				_t1 = new TestTask(1, 2, 1);
				_q.Enqueue(_t1);
			}

			[Fact]
			public void queue_becomes_empty() {
				_q.Process(max: 2);

				Assert.True(_q.IsEmpty);
			}

			[Fact]
			public void process_returns_true() {
				var processed = _q.Process(max: 2);

				Assert.True(processed);
			}
		}

		public class when_enqueuing_two_related_one_step_tasks {
			private StagedProcessingQueue _q;
			private TestTask _t1;
			private TestTask _t2;

			public when_enqueuing_two_related_one_step_tasks() {
				_q = new StagedProcessingQueue(new[] {true, true});
				_t1 = new TestTask(1, 1);
				_t2 = new TestTask(1, 1);
				_q.Enqueue(_t1);
				_q.Enqueue(_t2);
			}

			[Fact]
			public void two_process_execute_only_the_first_task() {
				_q.Process();
				_q.Process();

				Assert.True(_t1.StartedOn(0));
				Assert.True(!_t2.StartedOn(0));
			}
		}

		public class when_enqueuing_two_related_one_step_tasks_one_by_one {
			private StagedProcessingQueue _q;
			private TestTask _t1;
			private TestTask _t2;

			public when_enqueuing_two_related_one_step_tasks_one_by_one() {
				_q = new StagedProcessingQueue(new[] {true, true});
				_t1 = new TestTask(1, 1, 1);
				_t2 = new TestTask(1, 1, 1);
				_q.Enqueue(_t1);
				_q.Process();
				_q.Enqueue(_t2);
				_q.Process();
			}

			[Fact]
			public void two_process_execute_only_the_first_task() {
				Assert.True(_t1.StartedOn(0));
				Assert.True(_t2.StartedOn(0));
			}
		}

		public class when_enqueuing_two_two_step_tasks_that_relate_on_first_stage {
			private StagedProcessingQueue _q;
			private TestTask _t1;
			private TestTask _t2;

			public when_enqueuing_two_two_step_tasks_that_relate_on_first_stage() {
				_q = new StagedProcessingQueue(new[] {true, true});
				_t1 = new TestTask(null, 2, stageCorrelations: new object[] {"a", "a"});
				_t2 = new TestTask(null, 2, stageCorrelations: new object[] {"a", "a"});
				_q.Enqueue(_t1);
				_q.Enqueue(_t2);
			}

			[Fact]
			public void first_task_starts_on_second_stage_on_first_stage_completion() {
				_q.Process();
				_q.Process();

				_t1.Complete();

				_q.Process();

				Assert.True(_t1.StartedOn(1));
			}

			[Fact]
			public void second_task_does_not_start_on_second_stage_on_first_stage_completion() {
				_q.Process();
				_q.Process();

				_t2.Complete();

				_q.Process();
				_q.Process();

				Assert.True(!_t2.StartedOn(1));
			}

			[Fact]
			public void second_task_does_not_start_on_both_task_completion_on_the_first_stage() {
				_q.Process();
				_q.Process();

				_t1.Complete();
				_t2.Complete();

				_q.Process();
				_q.Process();

				Assert.True(!_t2.StartedOn(1));
			}

			[Fact]
			public void second_task_starts_on_the_first_task_completion_on_the_first_stage() {
				_q.Process();
				_q.Process();

				_t1.Complete();
				_t2.Complete();

				_q.Process();
				_q.Process();

				_t1.Complete();

				_q.Process();


				Assert.True(_t2.StartedOn(1));
			}
		}

		public class when_enqueuing_two_two_step_tasks_and_the_second_completes_first {
			private StagedProcessingQueue _q;
			private TestTask _t1;
			private TestTask _t2;

			public when_enqueuing_two_two_step_tasks_and_the_second_completes_first() {
				_q = new StagedProcessingQueue(new[] {true, true});
				_t1 = new TestTask(1, 2);
				_t2 = new TestTask(2, 2, 0);
				_q.Enqueue(_t1);
				_q.Enqueue(_t2);
				_q.Process(max: 2);
				_q.Process(max: 2);
				_q.Process(max: 2);
			}

			[Fact]
			public void process_waits_for_the_first_task_to_complete() {
				Assert.True(_t1.StartedOn(0));
				Assert.True(_t2.StartedOn(0));
			}

			[Fact]
			public void first_task_completed_unblocks_both_tasks() {
				_t1.Complete();
				_q.Process();
				_q.Process();

				Assert.True(_t1.StartedOn(1));
				Assert.True(_t2.StartedOn(1));
			}
		}

		public class when_enqueuing_two_async_async_sync_step_tasks_and_the_second_completes_first {
			private StagedProcessingQueue _q;
			private TestTask _t1;
			private TestTask _t2;

			public when_enqueuing_two_async_async_sync_step_tasks_and_the_second_completes_first() {
				_q = new StagedProcessingQueue(new[] {false, false, true});
				_t1 = new TestTask(1, 3);
				_t2 = new TestTask(2, 3, 0);
				_q.Enqueue(_t1);
				_q.Enqueue(_t2);
				_q.Process(max: 3);
				_q.Process(max: 3);
				_q.Process(max: 3);
			}

			[Fact]
			public void start_processing_second_task_on_stage_one() {
				Assert.True(_t1.StartedOn(0));
				Assert.True(_t2.StartedOn(1));
			}

			[Fact]
			public void first_task_completed_unblocks_both_tasks() {
				_t1.Complete();
				_q.Process();
				_q.Process();

				Assert.True(_t1.StartedOn(1));
				Assert.True(_t2.StartedOn(1));
			}
		}

		public class when_enqueuing_three_async_async_sync_step_tasks_and_they_complete_starting_from_second {
			private StagedProcessingQueue _q;
			private TestTask _t1;
			private TestTask _t2;
			private TestTask _t3;

			public when_enqueuing_three_async_async_sync_step_tasks_and_they_complete_starting_from_second() {
				_q = new StagedProcessingQueue(new[] {false, false, true});
				_t1 = new TestTask(1, 3);
				_t2 = new TestTask(2, 3, 0);
				_t3 = new TestTask(3, 3, 0);
				_q.Enqueue(_t1);
				_q.Enqueue(_t2);
				_q.Enqueue(_t3);
				_q.Process(max: 3);
				_q.Process(max: 3);
				_q.Process(max: 3);
			}

			[Fact]
			public void start_processing_second_and_third_tasks_on_stage_one() {
				Assert.True(_t1.StartedOn(0));
				Assert.True(_t2.StartedOn(1));
				Assert.True(_t3.StartedOn(1));
			}

			[Fact]
			public void first_task_keeps_other_blocked_at_stage_two() {
				_t1.Complete();
				_q.Process(max: 2);
				_q.Process(max: 2);
				_t2.Complete();
				_t3.Complete();
				_q.Process();
				Assert.True(!_t2.StartedOn(2));
				Assert.True(!_t3.StartedOn(2));
			}

			[Fact]
			public void first_task_completed_at_stage_one_unblock_all() {
				_t1.Complete();
				_q.Process();
				_q.Process();
				_t2.Complete();
				_t3.Complete();
				_q.Process();
				_t1.Complete();
				_q.Process();
				_q.Process();
				_q.Process();

				Assert.True(_t2.StartedOn(2));
				Assert.True(_t3.StartedOn(2));
			}
		}

		public class when_enqueuing_two_one_step_tasks {
			private StagedProcessingQueue _q;
			private TestTask _t1;
			private TestTask _t2;

			public when_enqueuing_two_one_step_tasks() {
				_q = new StagedProcessingQueue(new[] {true, true});
				_t1 = new TestTask(1, 1);
				_t2 = new TestTask(2, 1);
				_q.Enqueue(_t1);
				_q.Enqueue(_t2);
			}

			[Fact]
			public void two_process_execute_both_tasks_at_stage_zero() {
				_q.Process();
				_q.Process();

				Assert.True(_t1.StartedOn(0));
				Assert.True(_t2.StartedOn(0));
			}
		}


		public class when_changing_correlation_id_on_unordered_stage {
			private StagedProcessingQueue _q;
			private TestTask _t1;

			public when_changing_correlation_id_on_unordered_stage() {
				_q = new StagedProcessingQueue(new[] {false});
				_t1 = new TestTask(Guid.NewGuid(), 1, stageCorrelations: new object[] {"a"});
				_q.Enqueue(_t1);
			}

			[Fact]
			public void first_task_starts_on_second_stage_on_first_stage_completion() {
				_q.Process();
				Assert.Throws<InvalidOperationException>(() => { _t1.Complete(); });
			}
		}
	}
}
