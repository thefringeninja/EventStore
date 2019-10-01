using System.Collections.Generic;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class when_trying_to_get_oldest_entry {
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
		public void nothing_is_found_on_empty_stream(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var memTable = new HashListMemTable(version, maxSize: 10);
			memTable.Add(0x010100000000, 0x01, 0xffff);
			using (var ptable = PTable.FromMemtable(memTable, fixture.FileName, skipIndexVerify: skipIndexVerify)) {
				IndexEntry entry;
				Assert.False(ptable.TryGetOldestEntry(0x12, out entry));
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void single_item_is_latest(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var memTable = new HashListMemTable(version, maxSize: 10);
			memTable.Add(0x010100000000, 0x01, 0xffff);
			using (var ptable = PTable.FromMemtable(memTable, fixture.FileName, skipIndexVerify: skipIndexVerify)) {
				IndexEntry entry;
				Assert.True(ptable.TryGetOldestEntry(0x010100000000, out entry));
				Assert.Equal(fixture.GetHash(0x010100000000), entry.Stream);
				Assert.Equal(0x01, entry.Version);
				Assert.Equal(0xffff, entry.Position);
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void correct_entry_is_returned(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var memTable = new HashListMemTable(version, maxSize: 10);
			memTable.Add(0x010100000000, 0x01, 0xffff);
			memTable.Add(0x010100000000, 0x02, 0xfff2);
			using (var ptable = PTable.FromMemtable(memTable, fixture.FileName, skipIndexVerify: skipIndexVerify)) {
				IndexEntry entry;
				Assert.True(ptable.TryGetOldestEntry(0x010100000000, out entry));
				Assert.Equal(fixture.GetHash(0x010100000000), entry.Stream);
				Assert.Equal(0x01, entry.Version);
				Assert.Equal(0xffff, entry.Position);
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_duplicated_entries_exist_the_one_with_oldest_position_is_returned(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var memTable = new HashListMemTable(version, maxSize: 10);
			memTable.Add(0x010100000000, 0x01, 0xfff1);
			memTable.Add(0x010100000000, 0x02, 0xfff2);
			memTable.Add(0x010100000000, 0x01, 0xfff3);
			memTable.Add(0x010100000000, 0x02, 0xfff4);
			using (var ptable = PTable.FromMemtable(memTable, fixture.FileName, skipIndexVerify: skipIndexVerify)) {
				IndexEntry entry;
				Assert.True(ptable.TryGetOldestEntry(0x010100000000, out entry));
				Assert.Equal(fixture.GetHash(0x010100000000), entry.Stream);
				Assert.Equal(0x01, entry.Version);
				Assert.Equal(0xfff1, entry.Position);
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void only_entry_with_smallest_position_is_returned_when_triduplicated(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var memTable = new HashListMemTable(version, maxSize: 10);
			memTable.Add(0x010100000000, 0x01, 0xfff1);
			memTable.Add(0x010100000000, 0x01, 0xfff3);
			memTable.Add(0x010100000000, 0x01, 0xfff5);
			using (var ptable = PTable.FromMemtable(memTable, fixture.FileName, skipIndexVerify: skipIndexVerify)) {
				IndexEntry entry;
				Assert.True(ptable.TryGetOldestEntry(0x010100000000, out entry));
				Assert.Equal(fixture.GetHash(0x010100000000), entry.Stream);
				Assert.Equal(0x01, entry.Version);
				Assert.Equal(0xfff1, entry.Position);
			}
		}

		class Fixture : FileFixture {
			protected byte _ptableVersion = PTableVersions.IndexV1;

			private bool _skipIndexVerify;

			public Fixture(byte version, bool skipIndexVerify) {
				_ptableVersion = version;
				_skipIndexVerify = skipIndexVerify;
			}

			public ulong GetHash(ulong value) => _ptableVersion == PTableVersions.IndexV1 ? value >> 32 : value;
		}
	}
}
