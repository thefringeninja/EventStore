using System;
using System.Collections.Generic;
using EventStore.Core.Index;
using Xunit;
using EventStore.Core.Index.Hashes;
using System.IO;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class when_merging_ptables {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {false};
			yield return new object[] {true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void merged_ptable_is_32bit(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(PTableVersions.IndexV1, fixture.NewTable.Version);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_8_records_in_the_merged_index(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(8, fixture.NewTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void no_entries_should_have_upgraded_hashes(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			foreach (var item in fixture.NewTable.IterateAllInOrder()) {
				Assert.True((ulong)item.Position == item.Stream);
			}
		}

		class Fixture : DirectoryFixture {
			private readonly List<string> _files = new List<string>();
			private readonly List<PTable> _tables = new List<PTable>();

			public readonly PTable NewTable;

			public Fixture(bool skipIndexVerify) {
				_files.Add(GetTempFilePath());
				var table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010100000000, 0, 0x0101);
				table.Add(0x010200000000, 0, 0x0102);
				table.Add(0x010300000000, 0, 0x0103);
				table.Add(0x010400000000, 0, 0x0104);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010500000000, 0, 0x0105);
				table.Add(0x010600000000, 0, 0x0106);
				table.Add(0x010700000000, 0, 0x0107);
				table.Add(0x010800000000, 0, 0x0108);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				NewTable = PTable.MergeTo(_tables, GetTempFilePath(), (streamId, hash) => hash + 1, x => true,
					x => new Tuple<string, bool>(x.Stream.ToString(), true), PTableVersions.IndexV1,
					skipIndexVerify: skipIndexVerify);
			}

			public override void Dispose() {
				NewTable.Dispose();
				foreach (var ssTable in _tables) {
					ssTable.Dispose();
				}

				base.Dispose();
			}
		}
	}

	public class when_merging_ptables_to_64bit {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {false};
			yield return new object[] {true};
		}


		[Theory, MemberData(nameof(TestCases))]
		public void merged_ptable_is_64bit(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(PTableVersions.IndexV3, fixture.NewTable.Version);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_8_records_in_the_merged_index(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(8, fixture.NewTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void all_the_entries_have_upgraded_hashes(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			foreach (var item in fixture.NewTable.IterateAllInOrder()) {
				Assert.True((ulong)item.Position == item.Stream - 1);
			}
		}

		class Fixture : DirectoryFixture {
			private readonly List<string> _files = new List<string>();
			private readonly List<PTable> _tables = new List<PTable>();

			public readonly PTable NewTable;

			public Fixture(bool skipIndexVerify) {
				_files.Add(GetTempFilePath());
				var table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010100000000, 0, 0x0101);
				table.Add(0x010200000000, 0, 0x0102);
				table.Add(0x010300000000, 0, 0x0103);
				table.Add(0x010400000000, 0, 0x0104);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010500000000, 0, 0x0105);
				table.Add(0x010600000000, 0, 0x0106);
				table.Add(0x010700000000, 0, 0x0107);
				table.Add(0x010800000000, 0, 0x0108);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				NewTable = PTable.MergeTo(_tables, GetTempFilePath(), (streamId, hash) => hash + 1, x => true,
					x => new Tuple<string, bool>(x.Stream.ToString(), true), PTableVersions.IndexV3,
					skipIndexVerify: skipIndexVerify);
			}

			public override void Dispose() {
				NewTable.Dispose();
				foreach (var ssTable in _tables) {
					ssTable.Dispose();
				}

				base.Dispose();
			}
		}
	}

	public class when_merging_2_32bit_ptables_and_1_64bit_ptable_to_64bit {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {false};
			yield return new object[] {true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void merged_ptable_is_64bit(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(PTableVersions.IndexV2, fixture.NewTable.Version);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_12_records_in_the_merged_index(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(12, fixture.NewTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void only_the_32_bit_index_entries_should_have_upgraded_hashes(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			foreach (var item in fixture.NewTable.IterateAllInOrder()) {
				if (item.Position >= 0x010900000000) //these are 64bit already
				{
					Assert.True((ulong)item.Position == item.Stream);
				} else {
					Assert.True((ulong)item.Position >> 32 == item.Stream - 1);
				}
			}
		}

		class Fixture : DirectoryFixture {
			private readonly List<string> _files = new List<string>();
			private readonly List<PTable> _tables = new List<PTable>();

			public readonly PTable NewTable;

			public Fixture(bool skipIndexVerify) {
				_files.Add(GetTempFilePath());
				var table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010100000000, 0, 0x010100000000);
				table.Add(0x010200000000, 0, 0x010200000000);
				table.Add(0x010300000000, 0, 0x010300000000);
				table.Add(0x010400000000, 0, 0x010400000000);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010500000000, 0, 0x010500000000);
				table.Add(0x010600000000, 0, 0x010600000000);
				table.Add(0x010700000000, 0, 0x010700000000);
				table.Add(0x010800000000, 0, 0x010800000000);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				table = new HashListMemTable(PTableVersions.IndexV2, maxSize: 20);
				table.Add(0x010900000000, 0, 0x010900000000);
				table.Add(0x101000000000, 0, 0x101000000000);
				table.Add(0x111000000000, 0, 0x111000000000);
				table.Add(0x121000000000, 0, 0x121000000000);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				NewTable = PTable.MergeTo(_tables, GetTempFilePath(), (streamId, hash) => hash + 1, x => true,
					x => new Tuple<string, bool>(x.Stream.ToString(), true), PTableVersions.IndexV2,
					skipIndexVerify: skipIndexVerify);
			}

			public override void Dispose() {
				NewTable.Dispose();
				foreach (var ssTable in _tables) {
					ssTable.Dispose();
				}

				base.Dispose();
			}
		}
	}

	public class when_merging_1_32bit_ptables_and_1_64bit_ptable_with_missing_entries_to_64bit {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {false};
			yield return new object[] {true};
		}


		[Theory, MemberData(nameof(TestCases))]
		public void merged_ptable_is_64bit(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(PTableVersions.IndexV2, fixture.NewTable.Version);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_8_records_in_the_merged_index(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			// 5 from the 64 bit table (existsAt doesn't get used)
			// 3 from the 32 bit table (3 even positions)
			Assert.Equal(8, fixture.NewTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_items_are_sorted(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			var last = new IndexEntry(ulong.MaxValue, 0, long.MaxValue);
			foreach (var item in fixture.NewTable.IterateAllInOrder()) {
				Assert.True((last.Stream == item.Stream ? last.Version > item.Version : last.Stream > item.Stream) ||
				            ((last.Stream == item.Stream && last.Version == item.Version) &&
				             last.Position > item.Position));
				last = item;
			}
		}

		class Fixture : DirectoryFixture {
			private readonly List<string> _files = new List<string>();
			private readonly List<PTable> _tables = new List<PTable>();
			private IHasher hasher;

			public readonly PTable NewTable;

			public Fixture(bool skipIndexVerify) {
				hasher = new Murmur3AUnsafe();
				_files.Add(GetTempFilePath());
				var table = new HashListMemTable(PTableVersions.IndexV2, maxSize: 20);
				table.Add(0x010100000000, 2, 5);
				table.Add(0x010200000000, 1, 6);
				table.Add(0x010200000000, 2, 7);
				table.Add(0x010400000000, 0, 8);
				table.Add(0x010400000000, 1, 9);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010100000000, 1, 10);
				table.Add(0x010100000000, 2, 11);
				table.Add(0x010500000000, 1, 12);
				table.Add(0x010500000000, 2, 13);
				table.Add(0x010500000000, 3, 14);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				NewTable = PTable.MergeTo(_tables, GetTempFilePath(),
					(streamId, hash) => hash << 32 | hasher.Hash(streamId), x => x.Position % 2 == 0,
					x => new Tuple<string, bool>(x.Stream.ToString(), x.Position % 2 == 0), PTableVersions.IndexV2,
					skipIndexVerify: skipIndexVerify);
			}

			public override void Dispose() {
				NewTable.Dispose();
				foreach (var ssTable in _tables) {
					ssTable.Dispose();
				}

				base.Dispose();
			}
		}
	}

	public class when_merging_2_32bit_ptables_and_1_64bit_ptable_with_missing_entries_to_64bit {

		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {false};
			yield return new object[] {true};
		}
		
		[Theory, MemberData(nameof(TestCases))]
		public void merged_ptable_is_64bit(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(PTableVersions.IndexV2, fixture.NewTable.Version);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_10_records_in_the_merged_index(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			// 5 from 64 bit (existsAt not called)
			// 2 from first table (2 even positions)
			// 3 from last table (3 even positions)
			Assert.Equal(10, fixture.NewTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_items_are_sorted(bool skipIndexVerify) {
			using var fixture = new Fixture(skipIndexVerify);
			var last = new IndexEntry(ulong.MaxValue, 0, long.MaxValue);
			foreach (var item in fixture.NewTable.IterateAllInOrder()) {
				Assert.True((last.Stream == item.Stream ? last.Version > item.Version : last.Stream > item.Stream) ||
				            ((last.Stream == item.Stream && last.Version == item.Version) &&
				             last.Position > item.Position));
				last = item;
			}
		}

		class Fixture : DirectoryFixture {
			private readonly List<string> _files = new List<string>();
			private readonly List<PTable> _tables = new List<PTable>();
			private IHasher hasher;

			public readonly PTable NewTable;

			public Fixture(bool skipIndexVerify) {
				hasher = new Murmur3AUnsafe();
				_files.Add(GetTempFilePath());
				var table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010100000000, 0, 1);
				table.Add(0x010200000000, 0, 2);
				table.Add(0x010300000000, 0, 3);
				table.Add(0x010300000000, 1, 4);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				table = new HashListMemTable(PTableVersions.IndexV2, maxSize: 20);
				table.Add(0x010100000000, 2, 5);
				table.Add(0x010200000000, 1, 6);
				table.Add(0x010200000000, 2, 7);
				table.Add(0x010400000000, 0, 8);
				table.Add(0x010400000000, 1, 9);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010100000000, 1, 10);
				table.Add(0x010100000000, 2, 11);
				table.Add(0x010500000000, 1, 12);
				table.Add(0x010500000000, 2, 13);
				table.Add(0x010500000000, 3, 14);
				_tables.Add(PTable.FromMemtable(table, GetTempFilePath()));
				NewTable = PTable.MergeTo(_tables, GetTempFilePath(),
					(streamId, hash) => hash << 32 | hasher.Hash(streamId), x => x.Position % 2 == 0,
					x => new Tuple<string, bool>(x.Stream.ToString(), x.Position % 2 == 0), PTableVersions.IndexV2,
					skipIndexVerify: skipIndexVerify);
			}


			public override void Dispose() {
				NewTable.Dispose();
				foreach (var ssTable in _tables) {
					ssTable.Dispose();
				}
				base.Dispose();
			}
		}
	}
}
