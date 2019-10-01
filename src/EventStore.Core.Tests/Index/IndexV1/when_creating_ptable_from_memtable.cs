using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class when_creating_ptable_from_memtable {
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
		public void null_file_throws_null_exception(byte version, bool skipIndexVerify) {
			Assert.Throws<ArgumentNullException>(() =>
				PTable.FromMemtable(new HashListMemTable(version, maxSize: 10), null,
					skipIndexVerify: skipIndexVerify));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void null_memtable_throws_null_exception(byte version, bool skipIndexVerify) {
			Assert.Throws<ArgumentNullException>(() =>
				PTable.FromMemtable(null, "C:\\foo.txt", skipIndexVerify: skipIndexVerify));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void wait_for_destroy_will_timeout(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version);
			var table = new HashListMemTable(version, maxSize: 10);
			table.Add(0x010100000000, 0x0001, 0x0001);
			var ptable = PTable.FromMemtable(table, fixture.FileName, skipIndexVerify: skipIndexVerify);
			Assert.Throws<TimeoutException>(() => ptable.WaitForDisposal(1));

			// tear down
			ptable.MarkForDestruction();
			ptable.WaitForDisposal(1000);
		}

		//[Fact]
		//public void non_power_of_two_throws_invalid_operation()
		//{
		//    var table = new HashListMemTable(_ptableVersion, );
		//    table.Add(0x010100000000, 0x0001, 0x0001);
		//    table.Add(0x010500000000, 0x0001, 0x0002);
		//    table.Add(0x010200000000, 0x0001, 0x0003);
		//    Assert.Throws<InvalidOperationException>(() => PTable.FromMemtable(table, "C:\\foo.txt"));
		//}

		[Theory, MemberData(nameof(TestCases))]
		public void the_file_gets_created(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version);
			var indexEntrySize = PTable.IndexEntryV4Size;
			if (version == PTableVersions.IndexV1) {
				indexEntrySize = PTable.IndexEntryV1Size;
			} else if (version == PTableVersions.IndexV2) {
				indexEntrySize = PTable.IndexEntryV2Size;
			} else if (version == PTableVersions.IndexV3) {
				indexEntrySize = PTable.IndexEntryV3Size;
			}

			var table = new HashListMemTable(version, maxSize: 10);
			table.Add(0x010100000000, 0x0001, 0x0001);
			table.Add(0x010500000000, 0x0001, 0x0002);
			table.Add(0x010200000000, 0x0001, 0x0003);
			table.Add(0x010200000000, 0x0002, 0x0003);
			using (var sstable = PTable.FromMemtable(table, fixture.FileName, skipIndexVerify: skipIndexVerify)) {
				var fileinfo = new FileInfo(fixture.FileName);
				var midpointsCached = PTable.GetRequiredMidpointCountCached(4, version);
				Assert.Equal(
					PTableHeader.Size + 4 * indexEntrySize + midpointsCached * indexEntrySize +
					PTableFooter.GetSize(version) + PTable.MD5Size, fileinfo.Length);
				var items = sstable.IterateAllInOrder().ToList();
				Assert.Equal(fixture.GetHash(0x010500000000), items[0].Stream);
				Assert.Equal(0x0001, items[0].Version);
				Assert.Equal(fixture.GetHash(0x010200000000), items[1].Stream);
				Assert.Equal(0x0002, items[1].Version);
				Assert.Equal(fixture.GetHash(0x010200000000), items[2].Stream);
				Assert.Equal(0x0001, items[2].Version);
				Assert.Equal(fixture.GetHash(0x010100000000), items[3].Stream);
				Assert.Equal(0x0001, items[3].Version);
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_hash_of_file_is_valid(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version);
			var table = new HashListMemTable(version, maxSize: 10);
			table.Add(0x010100000000, 0x0001, 0x0001);
			table.Add(0x010500000000, 0x0001, 0x0002);
			table.Add(0x010200000000, 0x0001, 0x0003);
			table.Add(0x010200000000, 0x0002, 0x0003);
			using (var unused = PTable.FromMemtable(table, fixture.FileName, skipIndexVerify: false)) {
			}
		}

		class Fixture : FileFixture {
			private readonly byte _ptableVersion;

			public Fixture(byte version) {
				_ptableVersion = version;
			}


			public ulong GetHash(ulong value) => _ptableVersion == PTableVersions.IndexV1 ? value >> 32 : value;
		}
	}
}
