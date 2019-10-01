using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class searching_ptable_with_usual_items {
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
		public void the_table_has_five_items(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			Assert.Equal(5, fixture.PTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_first_item_can_be_found(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.True(fixture.PTable.TryGetOneValue(0x010100000000, 0x0001, out position));
			Assert.Equal(0x0001, position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_second_item_can_be_found(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.True(fixture.PTable.TryGetOneValue(0x010200000000, 0x0001, out position));
			Assert.Equal(0x0003, position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_third_item_can_be_found(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.True(fixture.PTable.TryGetOneValue(0x010200000000, 0x0002, out position));
			Assert.Equal(0x0004, position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_fourth_item_can_be_found(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.True(fixture.PTable.TryGetOneValue(0x010200000000, 0x0002, out position));
			Assert.Equal(0x0004, position);
		}


		[Theory, MemberData(nameof(TestCases))]
		public void the_fifth_item_can_be_found(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.True(fixture.PTable.TryGetOneValue(0x010500000000, 0x0001, out position));
			Assert.Equal(0x0002, position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void non_existent_item_cannot_be_found(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			long position;
			Assert.False(fixture.PTable.TryGetOneValue(0x0106, 0x0001, out position));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void range_query_returns_correct_items(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			// for now events are returned in order from larger key to lower
			var items = fixture.PTable.GetRange(0x010200000000, 0x0000, 0x0010).ToArray();
			Assert.Equal(2, items.Length);
			Assert.Equal(items[1].Stream, fixture.GetHash(0x010200000000));
			Assert.Equal(0x0001, items[1].Version);
			Assert.Equal(0x0003, items[1].Position);
			Assert.Equal(items[0].Stream, fixture.GetHash(0x010200000000));
			Assert.Equal(0x0002, items[0].Version);
			Assert.Equal(0x0004, items[0].Position);
		}

		public void range_query_returns_correct_item1(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			var items = fixture.PTable.GetRange(0x010200000000, 0x0000, 0x0001).ToArray();
			Assert.Single(items);
			Assert.Equal(fixture.GetHash(0x010200000000), items[0].Stream);
			Assert.Equal(0x0001, items[0].Version);
			Assert.Equal(0x0003, items[0].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void range_query_returns_correct_item2(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			var items = fixture.PTable.GetRange(0x010200000000, 0x0002, 0x0010).ToArray();
			Assert.Single(items);
			Assert.Equal(items[0].Stream, fixture.GetHash(0x010200000000));
			Assert.Equal(0x0002, items[0].Version);
			Assert.Equal(0x0004, items[0].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void range_query_returns_no_items_when_no_stream_in_sstable(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			var items = fixture.PTable.GetRange(0x0104, 0x0000, 0x0010);
			Assert.Empty(items);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void range_query_returns_items_when_startkey_is_less_than_current_min(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			var items = fixture.PTable.GetRange(0x010100000000, 0x0000, 0x0010).ToArray();
			Assert.Single(items);
			Assert.Equal(items[0].Stream, fixture.GetHash(0x010100000000));
			Assert.Equal(0x0001, items[0].Version);
			Assert.Equal(0x0001, items[0].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void range_query_returns_items_when_endkey_is_greater_than_current_max(byte version, bool skipIndexVerify, int midpointCacheDepth) {
			using var fixture = new Fixture(version, skipIndexVerify, midpointCacheDepth);
			var items = fixture.PTable.GetRange(0x010500000000, 0x0000, 0x0010).ToArray();
			Assert.Single(items);
			Assert.Equal(items[0].Stream, fixture.GetHash(0x010500000000));
			Assert.Equal(0x0001, items[0].Version);
			Assert.Equal(0x0002, items[0].Position);
		}

		class Fixture : PTableReadFixture {
			public Fixture(byte ptableVersion, bool skipIndexVerify, int midpointCacheDepth)
				: base(ptableVersion, skipIndexVerify, midpointCacheDepth) {
			}

			protected override void AddItemsForScenario(IMemTable memTable) {
				memTable.Add(0x010100000000, 0x0001, 0x0001);
				memTable.Add(0x010500000000, 0x0001, 0x0002);
				memTable.Add(0x010200000000, 0x0001, 0x0003);
				memTable.Add(0x010200000000, 0x0002, 0x0004);
				memTable.Add(0x010300000000, 0x0001, 0x0005);
			}

			public ulong GetHash(ulong hash) => _ptableVersion == PTableVersions.IndexV1 ? hash >> 32 : hash;
		}
	}
}
