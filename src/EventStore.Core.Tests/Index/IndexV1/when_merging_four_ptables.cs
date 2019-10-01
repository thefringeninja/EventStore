using System.Collections.Generic;
using EventStore.Core.Index;
using Xunit;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class when_merging_four_ptables {
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
		public void there_are_forty_records_in_merged_index(byte version, bool skipIndexVerify) {
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

		class Fixture : DirectoryFixture {
			private readonly List<string> _files = new List<string>();
			private readonly List<PTable> _tables = new List<PTable>();
			public readonly PTable NewTable;

			public Fixture(byte version, bool skipIndexVerify) {
				IHasher hasher = new Murmur3AUnsafe();

				for (int i = 0; i < 4; i++) {
					_files.Add(GetTempFilePath());

					var table = new HashListMemTable(version, maxSize: 20);
					for (int j = 0; j < 10; j++) {
						table.Add((ulong)(0x010100000000 << (j + 1)), i + 1, i * j);
					}

					_tables.Add(PTable.FromMemtable(table, _files[i], skipIndexVerify: skipIndexVerify));
				}

				_files.Add(GetTempFilePath());
				NewTable = PTable.MergeTo(_tables, _files[4], (streamId, hash) => hash << 32 | hasher.Hash(streamId),
					_ => true, _ => new System.Tuple<string, bool>("", true), version,
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
