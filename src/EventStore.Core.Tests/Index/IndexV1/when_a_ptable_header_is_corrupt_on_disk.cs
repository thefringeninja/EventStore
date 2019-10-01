using System.Collections.Generic;
using System.IO;
using EventStore.Core.Exceptions;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class when_a_ptable_header_is_corrupt_on_disk {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV1};
			yield return new object[] {PTableVersions.IndexV2};
			yield return new object[] {PTableVersions.IndexV3};
			yield return new object[] {PTableVersions.IndexV4};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_hash_is_invalid(byte version) {
			using var fixture = new Fixture(version);
			var exc = Assert.Throws<CorruptIndexException>(() => fixture.Table = PTable.FromFile(fixture.CopiedFileName, 16, false));
			Assert.IsType<HashValidationException>(exc.InnerException);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void no_error_if_index_verification_disabled(byte version) {
			using var fixture = new Fixture(version);
			fixture.Table = PTable.FromFile(fixture.CopiedFileName, 16, true);
		}

		class Fixture : DirectoryFixture {
			private readonly string _filename;
			public PTable Table;
			public readonly string CopiedFileName;

			public Fixture(byte version) {
				_filename = GetTempFilePath();
				CopiedFileName = GetTempFilePath();

				var mtable = new HashListMemTable(version, maxSize: 10);
				mtable.Add(0x010100000000, 0x0001, 0x0001);
				mtable.Add(0x010500000000, 0x0001, 0x0002);
				Table = PTable.FromMemtable(mtable, _filename);
				Table.Dispose();
				File.Copy(_filename, CopiedFileName);
				using (var f = new FileStream(CopiedFileName, FileMode.Open, FileAccess.ReadWrite,
					FileShare.ReadWrite)) {
					f.Seek(22, SeekOrigin.Begin);
					f.WriteByte(0x22);
				}
			}

			public override void Dispose() {
				Table.MarkForDestruction();
				Table.WaitForDisposal(1000);
				File.Delete(_filename);
				File.Delete(CopiedFileName);
				base.Dispose();
			}
		}
	}
}
