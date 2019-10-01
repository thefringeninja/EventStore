using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.Core.Index;
using EventStore.Core.Index.Hashes;
using Xunit;

namespace EventStore.Core.Tests.Index.Scavenge {
	public class when_scavenging_a_v1_index {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV2, false};
			yield return new object[] {PTableVersions.IndexV2, true};
			yield return new object[] {PTableVersions.IndexV3, false};
			yield return new object[] {PTableVersions.IndexV3, true};
			yield return new object[] {PTableVersions.IndexV4, false};
			yield return new object[] {PTableVersions.IndexV4, true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void scavenged_ptable_is_new_version(byte version, bool skipIndexVerify) {
			var fixture = new Fixture(version, skipIndexVerify);
			Assert.Equal(fixture.NewVersion, fixture.NewTable.Version);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_2_records_in_the_merged_index(byte version, bool skipIndexVerify) {
			var fixture = new Fixture(version, skipIndexVerify);
			Assert.Equal(2, fixture.NewTable.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void remaining_entries_should_have_been_upgraded_to_64bit_hash(byte version, bool skipIndexVerify) {
			var fixture = new Fixture(version, skipIndexVerify);
			ulong entry1 = 0x0103;
			ulong entry2 = 0x0102;

			using (var enumerator = fixture.NewTable.IterateAllInOrder().GetEnumerator()) {
				Assert.True(enumerator.MoveNext());
				Assert.Equal(enumerator.Current.Stream, fixture.UpgradeHash(entry1.ToString(), entry1));

				Assert.True(enumerator.MoveNext());
				Assert.Equal(enumerator.Current.Stream, fixture.UpgradeHash(entry2.ToString(), entry2));
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_items_are_sorted(byte version, bool skipIndexVerify) {
			var fixture = new Fixture(version, skipIndexVerify);
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
			public readonly byte NewVersion;
			public readonly Func<string, ulong, ulong> UpgradeHash;
			private readonly PTable _oldTable;

			public Fixture(byte newVersion, bool skipIndexVerify) {
				NewVersion = newVersion;
				IHasher hasher = new Murmur3AUnsafe();

				var table = new HashListMemTable(PTableVersions.IndexV1, maxSize: 20);
				table.Add(0x010100000000, 0, 1);
				table.Add(0x010200000000, 0, 2);
				table.Add(0x010300000000, 0, 3);
				table.Add(0x010300000000, 1, 4);
				_oldTable = PTable.FromMemtable(table, GetTempFilePath());

				long spaceSaved;
				UpgradeHash = (streamId, hash) => hash << 32 | hasher.Hash(streamId);
				Func<IndexEntry, bool> existsAt = x => x.Position % 2 == 0;
				Func<IndexEntry, Tuple<string, bool>> readRecord = x =>
					new Tuple<string, bool>(x.Stream.ToString(), x.Position % 2 == 0);
				NewTable = PTable.Scavenged(_oldTable, GetTempFilePath(), UpgradeHash, existsAt, readRecord,
					NewVersion,
					out spaceSaved, skipIndexVerify: skipIndexVerify);
			}

			public override void Dispose() {
				_oldTable.Dispose();
				NewTable.Dispose();
				
				base.Dispose();
			}
		}
	}
}
