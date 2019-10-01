using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class ptable_range_query_testsPerTestFixture {
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
		public void range_query_of_non_existing_stream_returns_nothing(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x14, 0x01, 0x02).ToArray();
			Assert.Empty(list);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void range_query_of_non_existing_version_returns_nothing(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010100000000, 0x03, 0x05).ToArray();
			Assert.Empty(list);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void range_query_with_hole_returns_items_included(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x01, 0x05).ToArray();
			Assert.Equal(3, list.Length);
			Assert.Equal(fixture.GetHash(0x010300000000), list[0].Stream);
			Assert.Equal(0x05, list[0].Version);
			Assert.Equal(0xfff5, list[0].Position);
			Assert.Equal(fixture.GetHash(0x010300000000), list[1].Stream);
			Assert.Equal(0x03, list[1].Version);
			Assert.Equal(0xfff3, list[1].Position);
			Assert.Equal(fixture.GetHash(0x010300000000), list[2].Stream);
			Assert.Equal(0x01, list[2].Version);
			Assert.Equal(0xfff1, list[2].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void query_with_start_in_range_but_not_end_results_returns_items_included(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x01, 0x04).ToArray();
			Assert.Equal(2, list.Length);
			Assert.Equal(fixture.GetHash(0x010300000000), list[0].Stream);
			Assert.Equal(0x03, list[0].Version);
			Assert.Equal(0xfff3, list[0].Position);
			Assert.Equal(fixture.GetHash(0x010300000000), list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff1, list[1].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void query_with_end_in_range_but_not_start_results_returns_items_included(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x00, 0x03).ToArray();
			Assert.Equal(2, list.Length);
			Assert.Equal(fixture.GetHash(0x010300000000), list[0].Stream);
			Assert.Equal(0x03, list[0].Version);
			Assert.Equal(0xfff3, list[0].Position);
			Assert.Equal(fixture.GetHash(0x010300000000), list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff1, list[1].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void query_with_end_and_start_exclusive_results_returns_items_included(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x00, 0x06).ToArray();
			Assert.Equal(3, list.Length);
			Assert.Equal(fixture.GetHash(0x010300000000), list[0].Stream);
			Assert.Equal(0x05, list[0].Version);
			Assert.Equal(0xfff5, list[0].Position);
			Assert.Equal(fixture.GetHash(0x010300000000), list[1].Stream);
			Assert.Equal(0x03, list[1].Version);
			Assert.Equal(0xfff3, list[1].Position);
			Assert.Equal(fixture.GetHash(0x010300000000), list[2].Stream);
			Assert.Equal(0x01, list[2].Version);
			Assert.Equal(0xfff1, list[2].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void query_with_end_inside_the_hole_in_list_returns_items_included(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x00, 0x04).ToArray();
			Assert.Equal(2, list.Length);
			Assert.Equal(fixture.GetHash(0x010300000000), list[0].Stream);
			Assert.Equal(0x03, list[0].Version);
			Assert.Equal(0xfff3, list[0].Position);
			Assert.Equal(fixture.GetHash(0x010300000000), list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff1, list[1].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void query_with_start_inside_the_hole_in_list_returns_items_included(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x02, 0x06).ToArray();
			Assert.Equal(2, list.Length);
			Assert.Equal(fixture.GetHash(0x010300000000), list[0].Stream);
			Assert.Equal(0x05, list[0].Version);
			Assert.Equal(0xfff5, list[0].Position);
			Assert.Equal(fixture.GetHash(0x010300000000), list[1].Stream);
			Assert.Equal(0x03, list[1].Version);
			Assert.Equal(0xfff3, list[1].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void query_with_start_and_end_inside_the_hole_in_list_returns_items_included(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x02, 0x04).ToArray();
			Assert.Single(list);
			Assert.Equal(fixture.GetHash(0x010300000000), list[0].Stream);
			Assert.Equal(0x03, list[0].Version);
			Assert.Equal(0xfff3, list[0].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void query_with_start_and_end_less_than_all_items_returns_nothing(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x00, 0x00).ToArray();
			Assert.Empty(list);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void query_with_start_and_end_greater_than_all_items_returns_nothing(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var list = fixture.PTable.GetRange(0x010300000000, 0x06, 0x06).ToArray();
			Assert.Empty(list);
		}

		class Fixture : FileFixture {
			private readonly byte _version;
			public readonly PTable PTable;

			public Fixture(byte version, bool skipIndexVerify) {
				_version = version;
				var table = new HashListMemTable(version, maxSize: 50);
				table.Add(0x010100000000, 0x0001, 0x0001);
				table.Add(0x010500000000, 0x0001, 0x0002);
				table.Add(0x010200000000, 0x0001, 0x0003);
				table.Add(0x010200000000, 0x0002, 0x0004);
				table.Add(0x010300000000, 0x0001, 0xFFF1);
				table.Add(0x010300000000, 0x0003, 0xFFF3);
				table.Add(0x010300000000, 0x0005, 0xFFF5);
				PTable = PTable.FromMemtable(table, FileName, cacheDepth: 0, skipIndexVerify: skipIndexVerify);
			}

			public ulong GetHash(ulong value) => _version == PTableVersions.IndexV1 ? value >> 32 : value;

			public override void Dispose() {
				PTable.Dispose();
				base.Dispose();
			}
		}
	}
}
