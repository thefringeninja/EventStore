using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Common.Utils;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;
using System.Linq;

namespace EventStore.Core.Tests.ClientAPI {
	[TestFixture, Category("ClientAPI"), Category("LongRunning")]
	public class transaction : SpecificationWithDirectoryPerTestFixture {
		private MiniNode _node;

		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName);
			await _node.Start();
		}

		[OneTimeTearDown]
		public override Task TestFixtureTearDown() {
			_node.Shutdown();
			return base.TestFixtureTearDown();
		}

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		[Test]
		[Category("Network")]
		public async Task should_start_on_non_existing_stream_with_correct_exp_ver_and_create_stream_on_commitAsync() {
			const string stream =
				"should_start_on_non_existing_stream_with_correct_exp_ver_and_create_stream_on_commit";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, ExpectedVersion.NoStream)) {
                    await transaction.WriteAsync(TestEvent.NewTestEvent());
					Assert.AreEqual(0, (await transaction.CommitAsync()).NextExpectedVersion);
				}
			}
		}

		[Test]
		[Category("Network")]
		public async Task should_start_on_non_existing_stream_with_exp_ver_any_and_create_stream_on_commitAsync() {
			const string stream = "should_start_on_non_existing_stream_with_exp_ver_any_and_create_stream_on_commit";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, ExpectedVersion.Any)) {
                    await transaction.WriteAsync(TestEvent.NewTestEvent());
					Assert.AreEqual(0, (await transaction.CommitAsync()).NextExpectedVersion);
				}
			}
		}

		[Test]
		[Category("Network")]
		public async Task should_fail_to_commit_non_existing_stream_with_wrong_exp_verAsync() {
			const string stream = "should_fail_to_commit_non_existing_stream_with_wrong_exp_ver";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, 1)) {
                    await transaction.WriteAsync(TestEvent.NewTestEvent());
                    Assert.ThrowsAsync<WrongExpectedVersionException>(() => transaction.CommitAsync());
				}
			}
		}

		[Test]
		[Category("Network")]
		public async Task should_do_nothing_if_commits_no_events_to_empty_streamAsync() {
			const string stream = "should_do_nothing_if_commits_no_events_to_empty_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, ExpectedVersion.NoStream)) {
					Assert.AreEqual(-1, (await transaction.CommitAsync()).NextExpectedVersion);
				}

				var result = await store.ReadStreamEventsForwardAsync(stream, 0, 1, resolveLinkTos: false);
				Assert.That(result.Events.Length, Is.EqualTo(0));
			}
		}

		[Test, Category("Network")]
		public async Task should_do_nothing_if_transactionally_writing_no_events_to_empty_streamAsync() {
			const string stream = "should_do_nothing_if_transactionally_writing_no_events_to_empty_stream";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, ExpectedVersion.NoStream)) {
                    await transaction.WriteAsync();
					Assert.AreEqual(-1, (await transaction.CommitAsync()).NextExpectedVersion);
				}

				var result = await store.ReadStreamEventsForwardAsync(stream, 0, 1, resolveLinkTos: false);
				Assert.That(result.Events.Length, Is.EqualTo(0));
			}
		}

		[Test]
		[Category("Network")]
		public async Task should_validate_expectations_on_commitAsync() {
			const string stream = "should_validate_expectations_on_commit";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, 100500)) {
					await transaction.WriteAsync(TestEvent.NewTestEvent());
					Assert.ThrowsAsync<WrongExpectedVersionException>(() => transaction.CommitAsync());
				}
			}
		}

		[Test]
		[Category("Network")]
		public async Task should_commit_when_writing_with_exp_ver_any_even_while_somene_is_writing_in_parallelAsync() {
			const string stream =
				"should_commit_when_writing_with_exp_ver_any_even_while_somene_is_writing_in_parallel";

			var transWritesCompleted = new ManualResetEventSlim(false);
			var writesToSameStreamCompleted = new ManualResetEventSlim(false);

			const int totalTranWrites = 500;
			const int totalPlainWrites = 500;

			//500 events during transaction
			ThreadPool.QueueUserWorkItem(_ => {
				Assert.DoesNotThrow(() => {
					using (var store = BuildConnection(_node)) {
						store.ConnectAsync().Wait();
						using (var transaction = store.StartTransactionAsync(stream, ExpectedVersion.Any).Result) {
							var writes = new List<Task>();
							for (int i = 0; i < totalTranWrites; i++) {
								writes.Add(transaction.WriteAsync(TestEvent.NewTestEvent(i.ToString(), "trans write")));
							}

							Task.WaitAll(writes.ToArray());
							transaction.CommitAsync().Wait();
							transWritesCompleted.Set();
						}
					}
				});
			});

			//500 events to same stream in parallel
			ThreadPool.QueueUserWorkItem(_ => {
				Assert.DoesNotThrow(() => {
					using (var store = BuildConnection(_node)) {
						store.ConnectAsync().Wait();
						var writes = new List<Task>();
						for (int i = 0; i < totalPlainWrites; i++) {
							writes.Add(store.AppendToStreamAsync(stream,
								ExpectedVersion.Any,
								new[] {TestEvent.NewTestEvent(i.ToString(), "plain write")}));
						}

						Task.WaitAll(writes.ToArray());
						writesToSameStreamCompleted.Set();
					}
				});
			});

			transWritesCompleted.Wait();
			writesToSameStreamCompleted.Wait();

			// check all written
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				var slice = await store.ReadStreamEventsForwardAsync(stream, 0, totalTranWrites + totalPlainWrites, false)
;
				Assert.That(slice.Events.Length, Is.EqualTo(totalTranWrites + totalPlainWrites));

				Assert.That(slice.Events.Count(ent => Helper.UTF8NoBom.GetString(ent.Event.Metadata) == "trans write"),
					Is.EqualTo(totalTranWrites));
				Assert.That(slice.Events.Count(ent => Helper.UTF8NoBom.GetString(ent.Event.Metadata) == "plain write"),
					Is.EqualTo(totalPlainWrites));
			}
		}

		[Test]
		[Category("Network")]
		public async Task should_fail_to_commit_if_started_with_correct_ver_but_committing_with_bad() {
			const string stream = "should_fail_to_commit_if_started_with_correct_ver_but_committing_with_bad";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, ExpectedVersion.NoStream)) {
                    await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent())
;
                    await transaction.WriteAsync(TestEvent.NewTestEvent());
                    Assert.ThrowsAsync<WrongExpectedVersionException>(() => transaction.CommitAsync());
				}
			}
		}

		[Test, Category("Network")]
		public async Task should_not_fail_to_commit_if_started_with_wrong_ver_but_committing_with_correct_ver() {
			const string stream = "should_not_fail_to_commit_if_started_with_wrong_ver_but_committing_with_correct_ver";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, 0)) {
                    await store.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent())
;
                    await transaction.WriteAsync(TestEvent.NewTestEvent());
                    var writeResult = await transaction.CommitAsync();
                    Assert.AreEqual(1, writeResult.NextExpectedVersion);
				}
			}
		}

		[Test]
		[Category("Network")]
		public async Task should_fail_to_commit_if_started_with_correct_ver_but_on_commit_stream_was_deleted() {
			const string stream = "should_fail_to_commit_if_started_with_correct_ver_but_on_commit_stream_was_deleted";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();
				using (var transaction = await store.StartTransactionAsync(stream, ExpectedVersion.NoStream)) {
                    await transaction.WriteAsync(TestEvent.NewTestEvent());
                    await store.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);
                    Assert.ThrowsAsync<StreamDeletedException>(() => transaction.CommitAsync());
				}
			}
		}

		[Test, Category("LongRunning")]
		public async Task idempotency_is_correct_for_explicit_transactions_with_expected_version_any() {
			const string streamId = "idempotency_is_correct_for_explicit_transactions_with_expected_version_any";
			using (var store = BuildConnection(_node)) {
                await store.ConnectAsync();

				var e = new EventData(Guid.NewGuid(), "SomethingHappened", true,
					Helper.UTF8NoBom.GetBytes("{Value:42}"), null);

				var transaction1 = await store.StartTransactionAsync(streamId, ExpectedVersion.Any);
                await transaction1.WriteAsync(e);
				Assert.AreEqual(0, (await transaction1.CommitAsync()).NextExpectedVersion);

				var transaction2 = await store.StartTransactionAsync(streamId, ExpectedVersion.Any);
                await transaction2.WriteAsync(e);
                var writeResult = await transaction2.CommitAsync();
                Assert.AreEqual(0, writeResult.NextExpectedVersion);

				var res = await store.ReadStreamEventsForwardAsync(streamId, 0, 100, false);
				Assert.AreEqual(1, res.Events.Length);
				Assert.AreEqual(e.EventId, res.Events[0].Event.EventId);
			}
		}
	}
}
