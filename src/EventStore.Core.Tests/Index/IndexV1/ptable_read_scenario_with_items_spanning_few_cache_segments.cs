using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {

	public class searching_ptable_with_items_spanning_few_cache_segments {
		public static IEnumerable<object[]> TestCases() {
			// all items in cache
			yield return new object[] {PTableVersions.IndexV1, false, 10};
			yield return new object[] {PTableVersions.IndexV1, true, 10};
			yield return new object[] {PTableVersions.IndexV2, false, 10};
			yield return new object[] {PTableVersions.IndexV2, true, 10};
			yield return new object[] {PTableVersions.IndexV3, false, 10};
			yield return new object[] {PTableVersions.IndexV3, true, 10};
			yield return new object[] {PTableVersions.IndexV4, false, 10};
			yield return new object[] {PTableVersions.IndexV4, true, 10};
			
			// some items in cache
			yield return new object[] {PTableVersions.IndexV1, false, 0};
			yield return new object[] {PTableVersions.IndexV1, true, 0};
			yield return new object[] {PTableVersions.IndexV2, false, 0};
			yield return new object[] {PTableVersions.IndexV2, true, 0};
			yield return new object[] {PTableVersions.IndexV3, false, 0};
			yield return new object[] {PTableVersions.IndexV3, true, 0};
			yield return new object[] {PTableVersions.IndexV4, false, 0};
			yield return new object[] {PTableVersions.IndexV4, true, 0};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_table_has_five_items(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			Assert.Equal(5, fixture.PTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_smallest_items_can_be_found(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.True(fixture.PTable.TryGetOneValue(0x010100000000, 0, out position));
			Assert.Equal(0x0002, position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_smallest_items_are_returned_in_descending_order(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			var entries = fixture.PTable.GetRange(0x010100000000, 0, 0).ToArray();
			Assert.Equal(2, entries.Length);
			Assert.Equal(fixture.GetHash(0x010100000000), entries[0].Stream);
			Assert.Equal(0, entries[0].Version);
			Assert.Equal(0x0002, entries[0].Position);
			Assert.Equal(fixture.GetHash(0x010100000000), entries[1].Stream);
			Assert.Equal(0, entries[1].Version);
			Assert.Equal(0x0001, entries[1].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void try_get_latest_entry_for_smallest_hash_returns_correct_index_entry(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			IndexEntry entry;
			Assert.True(fixture.PTable.TryGetLatestEntry(0x010100000000, out entry));
			Assert.Equal(fixture.GetHash(0x010100000000), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0x0002, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void try_get_oldest_entry_for_smallest_hash_returns_correct_index_entry(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			IndexEntry entry;
			Assert.True(fixture.PTable.TryGetOldestEntry(0x010100000000, out entry));
			Assert.Equal(fixture.GetHash(0x010100000000), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0x0001, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_largest_items_can_be_found(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.True(fixture.PTable.TryGetOneValue(0x010500000000, 0, out position));
			Assert.Equal(0x0005, position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_largest_items_are_returned_in_descending_order(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			var entries = fixture.PTable.GetRange(0x010500000000, 0, 0).ToArray();
			Assert.Equal(3, entries.Length);
			Assert.Equal(fixture.GetHash(0x010500000000), entries[0].Stream);
			Assert.Equal(0, entries[0].Version);
			Assert.Equal(0x0005, entries[0].Position);
			Assert.Equal(fixture.GetHash(0x010500000000), entries[1].Stream);
			Assert.Equal(0, entries[1].Version);
			Assert.Equal(0x0004, entries[1].Position);
			Assert.Equal(fixture.GetHash(0x010500000000), entries[2].Stream);
			Assert.Equal(0, entries[2].Version);
			Assert.Equal(0x0003, entries[2].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void try_get_latest_entry_for_largest_hash_returns_correct_index_entry(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			IndexEntry entry;
			Assert.True(fixture.PTable.TryGetLatestEntry(0x010500000000, out entry));
			Assert.Equal(fixture.GetHash(0x010500000000), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0x0005, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void try_get_oldest_entry_for_largest_hash_returns_correct_index_entry(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			IndexEntry entry;
			Assert.True(fixture.PTable.TryGetOldestEntry(0x010500000000, out entry));
			Assert.Equal(fixture.GetHash(0x010500000000), entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0x0003, entry.Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void non_existent_item_cannot_be_found(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.False(fixture.PTable.TryGetOneValue(2, 0, out position));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void range_query_returns_nothing_for_nonexistent_stream(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			var entries = fixture.PTable.GetRange(0x010200000000, 0, long.MaxValue).ToArray();
			Assert.Equal(0, entries.Length);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void try_get_latest_entry_returns_nothing_for_nonexistent_stream(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			IndexEntry entry;
			Assert.False(fixture.PTable.TryGetLatestEntry(0x010200000000, out entry));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void try_get_oldest_entry_returns_nothing_for_nonexistent_stream(byte pTableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(pTableVersion, skipIndexVerify, midpointCacheDepth);
			IndexEntry entry;
			Assert.False(fixture.PTable.TryGetOldestEntry(0x010200000000, out entry));
		}

		class Fixture : PTableReadFixture {
			public Fixture(byte ptableVersion, bool skipIndexVerify, int midpointCacheDepth) :
				base(ptableVersion, skipIndexVerify, midpointCacheDepth) {
			}

			protected override void AddItemsForScenario(IMemTable memTable) {
				memTable.Add(0x010100000000, 0, 0x0001);
				memTable.Add(0x010100000000, 0, 0x0002);
				memTable.Add(0x010500000000, 0, 0x0003);
				memTable.Add(0x010500000000, 0, 0x0004);
				memTable.Add(0x010500000000, 0, 0x0005);
			}

			public ulong GetHash(ulong value) {
				return _ptableVersion == PTableVersions.IndexV1 ? value >> 32 : value;
			}
		}
	}
}
