using System.Collections.Generic;
using System.IO;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class destroying_ptable {
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
		public void the_file_is_deleted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			fixture.Table.WaitForDisposal(1000);
			Assert.False(File.Exists(fixture.FileName));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void wait_for_destruction_returns(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			fixture.Table.WaitForDisposal(1000);
		}

		class Fixture : FileFixture {
			public readonly PTable Table;

			public Fixture(byte version, bool skipIndexVerify) {
				var mtable = new HashListMemTable(version, maxSize: 10);
				mtable.Add(0x010100000000, 0x0001, 0x0001);
				mtable.Add(0x010500000000, 0x0001, 0x0002);
				Table = PTable.FromMemtable(mtable, FileName, skipIndexVerify: skipIndexVerify);
				Table.MarkForDestruction();
			}

			public override void Dispose() {
				Table.WaitForDisposal(1000);
				base.Dispose();
			}
		}
	}
}
