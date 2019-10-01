using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.TransactionLog;
using Xunit;
using EventStore.Core.Index.Hashes;
using System;
using System.Threading.Tasks;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.Tests.Index.IndexV3 {
	[Trait("Category", "LongRunning")]
	public class when_upgrading_index_to_64bit_stream_version : SpecificationWithDirectoryPerTestFixture {
		private TableIndex _tableIndex;
		private IHasher _lowHasher;
		private IHasher _highHasher;
		private string _indexDir;
		protected byte _ptableVersion;

		public when_upgrading_index_to_64bit_stream_version() {
			_ptableVersion = PTableVersions.IndexV3;
		}

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			_indexDir = PathName;
			var fakeReader = new TFReaderLease(new FakeIndexReader());
			_lowHasher = new XXHashUnsafe();
			_highHasher = new Murmur3AUnsafe();
			_tableIndex = new TableIndex(_indexDir, _lowHasher, _highHasher,
				() => new HashListMemTable(PTableVersions.IndexV2, maxSize: 5),
				() => fakeReader,
				PTableVersions.IndexV2,
				5,
				maxSizeForMemory: 5,
				maxTablesPerLevel: 2);
			_tableIndex.Initialize(long.MaxValue);

			_tableIndex.Add(1, "testStream-1", 0, 1);
			_tableIndex.Add(1, "testStream-2", 0, 2);
			_tableIndex.Add(1, "testStream-1", 1, 3);
			_tableIndex.Add(1, "testStream-2", 1, 4);
			_tableIndex.Add(1, "testStream-1", 2, 5);

			_tableIndex.Close(false);

			_tableIndex = new TableIndex(_indexDir, _lowHasher, _highHasher,
				() => new HashListMemTable(_ptableVersion, maxSize: 5),
				() => fakeReader,
				_ptableVersion,
				5,
				maxSizeForMemory: 5,
				maxTablesPerLevel: 2);
			_tableIndex.Initialize(long.MaxValue);

			_tableIndex.Add(1, "testStream-2", 2, 6);
			_tableIndex.Add(1, "testStream-1", 3, 7);
			_tableIndex.Add(1, "testStream-2", 3, 8);
			_tableIndex.Add(1, "testStream-1", 4, 9);
			_tableIndex.Add(1, "testStream-2", 4, 10);

			await Task.Delay(500);
		}

		public override Task TestFixtureTearDown() {
			_tableIndex.Close();

			return base.TestFixtureTearDown();
		}

		[Fact]
		public void should_have_entries_in_sorted_order() {
			var streamId = "testStream-2";
			var result = _tableIndex.GetRange(streamId, 0, 4).ToArray();
			var hash = (ulong)_lowHasher.Hash(streamId) << 32 | _highHasher.Hash(streamId);

			Assert.Equal(result.Count(), 5);

			Assert.Equal(result[0].Stream, hash);
			Assert.Equal(result[0].Version, 4);
			Assert.Equal(result[0].Position, 10);

			Assert.Equal(result[1].Stream, hash);
			Assert.Equal(result[1].Version, 3);
			Assert.Equal(result[1].Position, 8);

			Assert.Equal(result[2].Stream, hash);
			Assert.Equal(result[2].Version, 2);
			Assert.Equal(result[2].Position, 6);

			Assert.Equal(result[3].Stream, hash);
			Assert.Equal(result[3].Version, 1);
			Assert.Equal(result[3].Position, 4);

			Assert.Equal(result[4].Stream, hash);
			Assert.Equal(result[4].Version, 0);
			Assert.Equal(result[4].Position, 2);

			streamId = "testStream-1";
			result = _tableIndex.GetRange(streamId, 0, 4).ToArray();
			hash = (ulong)_lowHasher.Hash(streamId) << 32 | _highHasher.Hash(streamId);

			Assert.Equal(result.Count(), 5);

			Assert.Equal(result[0].Stream, hash);
			Assert.Equal(result[0].Version, 4);
			Assert.Equal(result[0].Position, 9);

			Assert.Equal(result[1].Stream, hash);
			Assert.Equal(result[1].Version, 3);
			Assert.Equal(result[1].Position, 7);

			Assert.Equal(result[2].Stream, hash);
			Assert.Equal(result[2].Version, 2);
			Assert.Equal(result[2].Position, 5);

			Assert.Equal(result[3].Stream, hash);
			Assert.Equal(result[3].Version, 1);
			Assert.Equal(result[3].Position, 3);

			Assert.Equal(result[4].Stream, hash);
			Assert.Equal(result[4].Version, 0);
			Assert.Equal(result[4].Position, 1);
		}
	}

	public class FakeIndexReader : ITransactionFileReader {
		public void Reposition(long position) {
			throw new NotImplementedException();
		}

		public SeqReadResult TryReadNext() {
			throw new NotImplementedException();
		}

		public SeqReadResult TryReadPrev() {
			throw new NotImplementedException();
		}

		public RecordReadResult TryReadAt(long position) {
			var record = (LogRecord)new PrepareLogRecord(position, Guid.NewGuid(), Guid.NewGuid(), 0, 0,
				position % 2 == 0 ? "testStream-2" : "testStream-1", -1, DateTime.UtcNow, PrepareFlags.None, "type",
				new byte[0], null);
			return new RecordReadResult(true, position + 1, record, 1);
		}

		public bool ExistsAt(long position) {
			return true;
		}
	}
}
