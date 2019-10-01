using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Index;
using EventStore.Core.Index.Hashes;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog;
using Xunit;

namespace EventStore.Core.Tests.Index.Scavenge {
	public class when_scavenging_a_table_index_cancelled_while_waiting_for_lock : SpecificationWithDirectoryPerTestFixture {
		private TableIndex _tableIndex;
		private IHasher _lowHasher;
		private IHasher _highHasher;
		private string _indexDir;
		private FakeTFScavengerLog _log;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			_indexDir = PathName;

			var fakeReader = new TFReaderLease(new FakeIndexReader());
			_lowHasher = new XXHashUnsafe();
			_highHasher = new Murmur3AUnsafe();
			_tableIndex = new TableIndex(_indexDir, _lowHasher, _highHasher,
				() => new HashListMemTable(PTableVersions.IndexV4, maxSize: 5),
				() => fakeReader,
				PTableVersions.IndexV4,
				5,
				maxSizeForMemory: 2,
				maxTablesPerLevel: 5);
			_tableIndex.Initialize(long.MaxValue);

			_tableIndex.Add(1, "testStream-1", 0, 0);
			_tableIndex.Add(1, "testStream-1", 1, 100);
			_tableIndex.Add(1, "testStream-1", 2, 200);
			_tableIndex.Add(1, "testStream-1", 3, 300);
			_tableIndex.Add(1, "testStream-1", 4, 400);
			_tableIndex.Add(1, "testStream-1", 5, 500);

			_log = new FakeTFScavengerLog();

			var cancellationTokenSource = new CancellationTokenSource();
			cancellationTokenSource.Cancel();

			Assert.Throws<OperationCanceledException>(() => _tableIndex.Scavenge(_log, cancellationTokenSource.Token));

			// Check it's loadable still.
			_tableIndex.Close(false);

			_tableIndex = new TableIndex(_indexDir, _lowHasher, _highHasher,
				() => new HashListMemTable(PTableVersions.IndexV4, maxSize: 5),
				() => fakeReader,
				PTableVersions.IndexV4,
				5,
				maxSizeForMemory: 2,
				maxTablesPerLevel: 5);

			_tableIndex.Initialize(long.MaxValue);
		}

		public override Task TestFixtureTearDown() {
			_tableIndex.Close();

			return base.TestFixtureTearDown();
		}

		[Fact]
		public void should_still_have_all_entries_in_sorted_order() {
			var streamId = "testStream-1";
			var result = _tableIndex.GetRange(streamId, 0, 5).ToArray();
			var hash = (ulong)_lowHasher.Hash(streamId) << 32 | _highHasher.Hash(streamId);

			Assert.Equal(result.Count(), 6);

			Assert.Equal(result[0].Stream, hash);
			Assert.Equal(result[0].Version, 5);
			Assert.Equal(result[0].Position, 500);

			Assert.Equal(result[1].Stream, hash);
			Assert.Equal(result[1].Version, 4);
			Assert.Equal(result[1].Position, 400);

			Assert.Equal(result[2].Stream, hash);
			Assert.Equal(result[2].Version, 3);
			Assert.Equal(result[2].Position, 300);

			Assert.Equal(result[3].Stream, hash);
			Assert.Equal(result[3].Version, 2);
			Assert.Equal(result[3].Position, 200);

			Assert.Equal(result[4].Stream, hash);
			Assert.Equal(result[4].Version, 1);
			Assert.Equal(result[4].Position, 100);

			Assert.Equal(result[5].Stream, hash);
			Assert.Equal(result[5].Version, 0);
			Assert.Equal(result[5].Position, 0);
		}
	}
}
