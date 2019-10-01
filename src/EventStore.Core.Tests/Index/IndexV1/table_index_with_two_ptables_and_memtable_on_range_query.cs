using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Index;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog;
using Xunit;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class table_index_with_two_ptables_and_memtable_on_range_query {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV1, false};
			yield return new object[] {PTableVersions.IndexV1, true};
			yield return new object[] {PTableVersions.IndexV2, false};
			yield return new object[] {PTableVersions.IndexV2, true};
			yield return new object[] {PTableVersions.IndexV3, false};
			yield return new object[] {PTableVersions.IndexV3, true};
			yield return new object[] {PTableVersions.IndexV4, false};
			yield return new object[] {PTableVersions.IndexV4, true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_not_return_latest_entry_for_nonexisting_stream(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;
			Assert.False(fixture.TableIndex.TryGetLatestEntry("7", out entry));
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_not_return_oldest_entry_for_nonexisting_stream(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;
			Assert.False(fixture.TableIndex.TryGetLatestEntry("7", out entry));
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_latest_entry_for_stream_with_latest_entry_in_memtable(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;
			Assert.True(fixture.TableIndex.TryGetLatestEntry("4", out entry));
			Assert.Equal(fixture.GetHash("4"), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0xFF01, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_latest_entry_for_stream_with_latest_entry_in_ptable_0(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;
			Assert.True(fixture.TableIndex.TryGetLatestEntry("1", out entry));
			Assert.Equal(fixture.GetHash("1"), entry.Stream);
			Assert.Equal(1, entry.Version);
			Assert.Equal(0xFF11, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_latest_entry_for_another_stream_with_latest_entry_in_ptable_0(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;
			Assert.True(fixture.TableIndex.TryGetLatestEntry("6", out entry));
			Assert.Equal(fixture.GetHash("6"), entry.Stream);
			Assert.Equal(1, entry.Version);
			Assert.Equal(0xFF01, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_latest_entry_for_stream_with_latest_entry_in_ptable_1(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;
			Assert.True(fixture.TableIndex.TryGetLatestEntry("5", out entry));
			Assert.Equal(fixture.GetHash("5"), entry.Stream);
			Assert.Equal(10, entry.Version);
			Assert.Equal(0xFFF1, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_latest_entry_for_stream_with_latest_entry_in_ptable_2(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;
			Assert.True(fixture.TableIndex.TryGetLatestEntry("2", out entry));
			Assert.Equal(fixture.GetHash("2"), entry.Stream);
			Assert.Equal(1, entry.Version);
			Assert.Equal(0xFF01, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_latest_entry_for_another_stream_with_latest_entry_in_ptable_2(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;
			Assert.True(fixture.TableIndex.TryGetLatestEntry("3", out entry));
			Assert.Equal(fixture.GetHash("3"), entry.Stream);
			Assert.Equal(1, entry.Version);
			Assert.Equal(0xFF03, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_oldest_entries_for_each_stream(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			IndexEntry entry;

			Assert.True(fixture.TableIndex.TryGetOldestEntry("1", out entry));
			Assert.Equal(fixture.GetHash("1"), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0xFF00, entry.Position);

			Assert.True(fixture.TableIndex.TryGetOldestEntry("2", out entry));
			Assert.Equal(fixture.GetHash("2"), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0xFF00, entry.Position);

			Assert.True(fixture.TableIndex.TryGetOldestEntry("3", out entry));
			Assert.Equal(fixture.GetHash("3"), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0xFF00, entry.Position);

			Assert.True(fixture.TableIndex.TryGetOldestEntry("4", out entry));
			Assert.Equal(fixture.GetHash("4"), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0xFF00, entry.Position);

			Assert.True(fixture.TableIndex.TryGetOldestEntry("5", out entry));
			Assert.Equal(fixture.GetHash("5"), entry.Stream);
			Assert.Equal(10, entry.Version);
			Assert.Equal(0xFFF1, entry.Position);

			Assert.True(fixture.TableIndex.TryGetOldestEntry("6", out entry));
			Assert.Equal(fixture.GetHash("6"), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0xFF00, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_empty_range_for_nonexisting_stream(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			var range = fixture.TableIndex.GetRange("7", 0, int.MaxValue).ToArray();
			Assert.Equal(0, range.Length);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_full_range_with_descending_order_for_1(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			var range = fixture.TableIndex.GetRange("1", 0, long.MaxValue).ToArray();
			Assert.Equal(4, range.Length);
			Assert.Equal(new IndexEntry(fixture.GetHash("1"), 1, 0xFF11), range[0]);
			Assert.Equal(new IndexEntry(fixture.GetHash("1"), 1, 0xFF01), range[1]);
			Assert.Equal(new IndexEntry(fixture.GetHash("1"), 0, 0xFF10), range[2]);
			Assert.Equal(new IndexEntry(fixture.GetHash("1"), 0, 0xFF00), range[3]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_full_range_with_descending_order_for_2(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			var range = fixture.TableIndex.GetRange("2", 0, long.MaxValue).ToArray();
			Assert.Equal(2, range.Length);
			Assert.Equal(new IndexEntry(fixture.GetHash("2"), 1, 0xFF01), range[0]);
			Assert.Equal(new IndexEntry(fixture.GetHash("2"), 0, 0xFF00), range[1]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_full_range_with_descending_order_for_3(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			var range = fixture.TableIndex.GetRange("3", 0, long.MaxValue).ToArray();
			Assert.Equal(4, range.Length);
			Assert.Equal(new IndexEntry(fixture.GetHash("3"), 1, 0xFF03), range[0]);
			Assert.Equal(new IndexEntry(fixture.GetHash("3"), 1, 0xFF01), range[1]);
			Assert.Equal(new IndexEntry(fixture.GetHash("3"), 0, 0xFF02), range[2]);
			Assert.Equal(new IndexEntry(fixture.GetHash("3"), 0, 0xFF00), range[3]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_full_range_with_descending_order_for_4(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			var range = fixture.TableIndex.GetRange("4", 0, long.MaxValue).ToArray();
			Assert.Equal(2, range.Length);
			Assert.Equal(new IndexEntry(fixture.GetHash("4"), 0, 0xFF01), range[0]);
			Assert.Equal(new IndexEntry(fixture.GetHash("4"), 0, 0xFF00), range[1]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_full_range_with_descending_order_for_5(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			var range = fixture.TableIndex.GetRange("5", 0, long.MaxValue).ToArray();
			Assert.Equal(1, range.Length);
			Assert.Equal(new IndexEntry(fixture.GetHash("5"), 10, 0xFFF1), range[0]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_correct_full_range_with_descending_order_for_6(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			var range = fixture.TableIndex.GetRange("6", 0, long.MaxValue).ToArray();
			Assert.Equal(2, range.Length);
			Assert.Equal(new IndexEntry(fixture.GetHash("6"), 1, 0xFF01), range[0]);
			Assert.Equal(new IndexEntry(fixture.GetHash("6"), 0, 0xFF00), range[1]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_not_return_one_value_for_nonexistent_stream(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			long pos;
			Assert.False(fixture.TableIndex.TryGetOneValue("7", 0, out pos));
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_return_one_value_for_existing_streams_for_existing_version(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			long pos;
			Assert.True(fixture.TableIndex.TryGetOneValue("1", 1, out pos));
			Assert.Equal(0xFF11, pos);

			Assert.True(fixture.TableIndex.TryGetOneValue("2", 0, out pos));
			Assert.Equal(0xFF00, pos);

			Assert.True(fixture.TableIndex.TryGetOneValue("3", 0, out pos));
			Assert.Equal(0xFF02, pos);

			Assert.True(fixture.TableIndex.TryGetOneValue("4", 0, out pos));
			Assert.Equal(0xFF01, pos);

			Assert.True(fixture.TableIndex.TryGetOneValue("5", 10, out pos));
			Assert.Equal(0xFFF1, pos);

			Assert.True(fixture.TableIndex.TryGetOneValue("6", 1, out pos));
			Assert.Equal(0xFF01, pos);
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_not_return_one_value_for_existing_streams_for_nonexistent_version(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			await Task.Delay(500);
			long pos;
			Assert.False(fixture.TableIndex.TryGetOneValue("1", 2, out pos));
			Assert.False(fixture.TableIndex.TryGetOneValue("2", 2, out pos));
			Assert.False(fixture.TableIndex.TryGetOneValue("3", 2, out pos));
			Assert.False(fixture.TableIndex.TryGetOneValue("4", 1, out pos));
			Assert.False(fixture.TableIndex.TryGetOneValue("5", 0, out pos));
			Assert.False(fixture.TableIndex.TryGetOneValue("6", 2, out pos));
		}

		class Fixture : DirectoryFixture {
			public TableIndex TableIndex;
			private IHasher _lowHasher;
			private IHasher _highHasher;
			private readonly byte _version;

			public Fixture(byte version, bool skipIndexVerify) {
				_version = version;
				var fakeReader = new TFReaderLease(new FakeIndexReader());
				_lowHasher = new FakeIndexHasher();
				_highHasher = new FakeIndexHasher();
				TableIndex = new TableIndex(PathName, _lowHasher, _highHasher,
					() => new HashListMemTable(version, maxSize: 10),
					() => fakeReader,
					version,
					5,
					maxSizeForMemory: 2,
					maxTablesPerLevel: 2,
					skipIndexVerify: skipIndexVerify);
				TableIndex.Initialize(long.MaxValue);

				// ptable level 2
				TableIndex.Add(0, "1", 0, 0xFF00);
				TableIndex.Add(0, "1", 1, 0xFF01);
				TableIndex.Add(0, "2", 0, 0xFF00);
				TableIndex.Add(0, "2", 1, 0xFF01);
				TableIndex.Add(0, "3", 0, 0xFF00);
				TableIndex.Add(0, "3", 1, 0xFF01);
				TableIndex.Add(0, "3", 0, 0xFF02);
				TableIndex.Add(0, "3", 1, 0xFF03);

				// ptable level 1
				TableIndex.Add(0, "4", 0, 0xFF00);
				TableIndex.Add(0, "5", 10, 0xFFF1);
				TableIndex.Add(0, "6", 0, 0xFF00);
				TableIndex.Add(0, "1", 0, 0xFF10);

				// ptable level 0
				TableIndex.Add(0, "6", 1, 0xFF01);
				TableIndex.Add(0, "1", 1, 0xFF11);

				// memtable
				TableIndex.Add(0, "4", 0, 0xFF01);
			}

			public override void Dispose() {
				TableIndex.Close();
				base.Dispose();
			}

			public ulong GetHash(string streamId) {
				ulong hash = _lowHasher.Hash(streamId);
				hash = _version == PTableVersions.IndexV1 ? hash : hash << 32 | _highHasher.Hash(streamId);
				return hash;
			}
		}
	}
}
