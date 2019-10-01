using System.Collections.Generic;
using EventStore.Core.Index;
using Xunit;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class when_merging_ptables_with_entries_to_nonexisting_record_in_newer_index_versions :
		SpecificationWithDirectoryPerTestFixture {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV2, false};
			yield return new object[] {PTableVersions.IndexV2, true};
			yield return new object[] {PTableVersions.IndexV3, false};
			yield return new object[] {PTableVersions.IndexV3, true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void all_entries_are_left(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Equal(40, fixture.NewTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_items_are_sorted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var last = new IndexEntry(ulong.MaxValue, 0, long.MaxValue);
			foreach (var item in fixture.NewTable.IterateAllInOrder()) {
				Assert.True((last.Stream == item.Stream ? last.Version > item.Version : last.Stream > item.Stream) ||
				            ((last.Stream == item.Stream && last.Version == item.Version) &&
				             last.Position > item.Position));
				last = item;
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_right_items_are_deleted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			for (int i = 0; i < 4; i++) {
				for (int j = 0; j < 10; j++) {
					Assert.True(fixture.NewTable.TryGetOneValue((ulong)(0x010100000000 << i), j, out var position));
					Assert.Equal(i * 10 + j, position);
				}
			}
		}

		protected class Fixture : DirectoryFixture {
			private readonly List<string> _files = new List<string>();
			private readonly List<PTable> _tables = new List<PTable>();
			public readonly PTable NewTable;

			public Fixture(byte version, bool skipIndexVerify) {
				for (int i = 0; i < 4; i++) {
					_files.Add(GetTempFilePath());

					var table = new HashListMemTable(version, maxSize: 30);
					for (int j = 0; j < 10; j++) {
						table.Add((ulong)(0x010100000000 << i), j, i * 10 + j);
					}

					_tables.Add(PTable.FromMemtable(table, _files[i], skipIndexVerify: skipIndexVerify));
				}

				_files.Add(GetTempFilePath());
				NewTable = PTable.MergeTo(_tables, _files[4], (streamId, hash) => hash, x => x.Position % 2 == 0,
					x => new Tuple<string, bool>("", x.Position % 2 == 0), version,
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
