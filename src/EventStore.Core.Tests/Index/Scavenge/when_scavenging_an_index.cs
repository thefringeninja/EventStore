using System;
using System.Collections.Generic;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.Scavenge {
	public class when_scavenging_an_index {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV2, false};
			yield return new object[] {PTableVersions.IndexV2, true};
			yield return new object[] {PTableVersions.IndexV3, false};
			yield return new object[] {PTableVersions.IndexV3, true};
			yield return new object[] {PTableVersions.IndexV4, false};
			yield return new object[] {PTableVersions.IndexV4, true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void scavenged_ptable_is_newest_version(byte oldVersion, bool skipIndexVerify) {
			using var fixture = new Fixture(oldVersion, skipIndexVerify);
			Assert.Equal(PTableVersions.IndexV4, fixture.NewTable.Version);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_2_records_in_the_merged_index(byte oldVersion, bool skipIndexVerify) {
			using var fixture = new Fixture(oldVersion, skipIndexVerify);
			Assert.Equal(2, fixture.NewTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_items_are_sorted(byte oldVersion, bool skipIndexVerify) {
			using var fixture = new Fixture(oldVersion, skipIndexVerify);
			var last = new IndexEntry(ulong.MaxValue, 0, long.MaxValue);
			foreach (var item in fixture.NewTable.IterateAllInOrder()) {
				Assert.True((last.Stream == item.Stream ? last.Version > item.Version : last.Stream > item.Stream) ||
				            ((last.Stream == item.Stream && last.Version == item.Version) &&
				             last.Position > item.Position));
				last = item;
			}
		}

		class Fixture : DirectoryFixture {
			public readonly PTable NewTable;
			private readonly PTable _oldTable;

			public Fixture(byte oldVersion, bool skipIndexVerify) {
				var table = new HashListMemTable(oldVersion, maxSize: 20);
				table.Add(0x010100000000, 0, 1);
				table.Add(0x010200000000, 0, 2);
				table.Add(0x010300000000, 0, 3);
				table.Add(0x010300000000, 1, 4);
				_oldTable = PTable.FromMemtable(table, GetTempFilePath());

				long spaceSaved;
				Func<IndexEntry, bool> existsAt = x => x.Position % 2 == 0;
				Func<IndexEntry, Tuple<string, bool>> readRecord = x => {
					throw new Exception("Should not be called");
				};
				Func<string, ulong, ulong> upgradeHash = (streamId, hash) => {
					throw new Exception("Should not be called");
				};

				NewTable = PTable.Scavenged(_oldTable, GetTempFilePath(), upgradeHash, existsAt, readRecord,
					PTableVersions.IndexV4, out spaceSaved, skipIndexVerify: skipIndexVerify);
			}

			public override void Dispose() {
				_oldTable.Dispose();
				NewTable.Dispose();
				base.Dispose();
			}
		}
	}
}
