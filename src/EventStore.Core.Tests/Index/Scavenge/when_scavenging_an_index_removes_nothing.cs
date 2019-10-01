using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.Scavenge {
	public class when_scavenging_an_index_removes_nothing {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV2, false};
			yield return new object[] {PTableVersions.IndexV2, true};
			yield return new object[] {PTableVersions.IndexV3, false};
			yield return new object[] {PTableVersions.IndexV3, true};
			yield return new object[] {PTableVersions.IndexV4, false};
			yield return new object[] {PTableVersions.IndexV4, true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void a_null_object_is_returned_if_the_version_is_unchanged(byte oldVersion, bool skipIndexVerify) {
			using var fixture = new Fixture(oldVersion, skipIndexVerify);
			if (fixture.OldVersion == PTableVersions.IndexV4) {
				Assert.Null(fixture.NewTable);
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_output_file_is_deleted_if_version_is_unchanged(byte oldVersion, bool skipIndexVerify) {
			using var fixture = new Fixture(oldVersion, skipIndexVerify);
			if (fixture.OldVersion == PTableVersions.IndexV4) {
				Assert.False(File.Exists(fixture.ExpectedOutputFile));
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void a_table_with_all_items_is_returned_with_a_newer_version(byte oldVersion, bool skipIndexVerify) {
			using var fixture = new Fixture(oldVersion, skipIndexVerify);
			if (fixture.OldVersion != PTableVersions.IndexV4) {
				Assert.NotNull(fixture.NewTable);
				Assert.Equal(4, fixture.NewTable.Count);
			}
		}

		class Fixture : DirectoryFixture {
			public readonly PTable NewTable;
			public readonly byte OldVersion;
			private readonly PTable _oldTable;
			public readonly string ExpectedOutputFile;

			public Fixture(byte oldVersion, bool skipIndexVerify) {
				OldVersion = oldVersion;
				var table = new HashListMemTable(OldVersion, maxSize: 20);
				table.Add(0x010100000000, 0, 1);
				table.Add(0x010200000000, 0, 2);
				table.Add(0x010300000000, 0, 3);
				table.Add(0x010300000000, 1, 4);
				_oldTable = PTable.FromMemtable(table, GetTempFilePath());

				long spaceSaved;
				Func<IndexEntry, bool> existsAt = x => true;
				Func<IndexEntry, Tuple<string, bool>> readRecord = x => {
					throw new Exception("Should not be called");
				};
				Func<string, ulong, ulong> upgradeHash = (streamId, hash) => {
					throw new Exception("Should not be called");
				};

				ExpectedOutputFile = GetTempFilePath();
				NewTable = PTable.Scavenged(_oldTable, ExpectedOutputFile, upgradeHash, existsAt, readRecord,
					PTableVersions.IndexV4, out spaceSaved, skipIndexVerify: skipIndexVerify);
			}

			public override void Dispose() {
				_oldTable.Dispose();
				NewTable?.Dispose();
				
				base.Dispose();
			}
		}
	}
}
