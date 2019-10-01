using System.Collections.Generic;
using System.IO;
using EventStore.Core.Exceptions;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class when_a_ptable_is_corrupt_on_disk {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV1};
			yield return new object[] {PTableVersions.IndexV2};
			yield return new object[] {PTableVersions.IndexV3};
			yield return new object[] {PTableVersions.IndexV4};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_hash_is_invalid(byte version) {
			using var fixture = new Fixture(version);
			var exc = Assert.Throws<CorruptIndexException>(() => PTable.FromFile(fixture.CopiedFileName, 16, false));
			Assert.IsType<HashValidationException>(exc.InnerException);
		}

		class Fixture : DirectoryFixture {
			private PTable _table;
			public readonly string CopiedFileName;

			public Fixture(byte version) {
				var filename = GetTempFilePath();
				CopiedFileName = GetTempFilePath();
				var mtable = new HashListMemTable(version, maxSize: 10);
				mtable.Add(0x010100000000, 0x0001, 0x0001);
				mtable.Add(0x010500000000, 0x0001, 0x0002);
				_table = PTable.FromMemtable(mtable, filename);
				_table.Dispose();
				File.Copy(filename, CopiedFileName);
				using (var f = new FileStream(CopiedFileName, FileMode.Open, FileAccess.ReadWrite,
					FileShare.ReadWrite)) {
					f.Seek(130, SeekOrigin.Begin);
					f.WriteByte(0x22);
				}
			}

			public override void Dispose() {
				_table.MarkForDestruction();
				_table.WaitForDisposal(1000);

				base.Dispose();
			}
		}
	}
}
