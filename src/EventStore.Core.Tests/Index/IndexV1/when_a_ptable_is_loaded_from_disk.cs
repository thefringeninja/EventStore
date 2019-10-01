using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using EventStore.Common.Options;
using EventStore.Core.Exceptions;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class when_a_ptable_is_loaded_from_disk {
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
		public void same_midpoints_are_loaded_when_enabling_or_disabling_index_verification(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			for (int depth = 2; depth <= 20; depth++) {
				var ptableWithMD5Verification = PTable.FromFile(fixture.CopiedFileName, depth, false);
				var ptableWithoutVerification = PTable.FromFile(fixture.CopiedFileName, depth, true);
				var midPoints1 = ptableWithMD5Verification.GetMidPoints();
				var midPoints2 = ptableWithoutVerification.GetMidPoints();

				Assert.Equal(midPoints1.Length, midPoints2.Length);
				for (var i = 0; i < midPoints1.Length; i++) {
					Assert.Equal(midPoints1[i].ItemIndex, midPoints2[i].ItemIndex);
					Assert.Equal(midPoints1[i].Key.Stream, midPoints2[i].Key.Stream);
					Assert.Equal(midPoints1[i].Key.Version, midPoints2[i].Key.Version);
				}

				ptableWithMD5Verification.Dispose();
				ptableWithoutVerification.Dispose();
			}
		}

		class Fixture : DirectoryFixture {
			public readonly string CopiedFileName;

			public Fixture(byte version, bool skipIndexVerify) {
				var filename = GetTempFilePath();
				CopiedFileName = GetTempFilePath();
				var mtable = new HashListMemTable(version, maxSize: 1024);

				long logPosition = 0;
				long eventNumber = 1;
				ulong streamId = 0x010100000000;

				for (var i = 0; i < 1337; i++) {
					logPosition++;

					if (i % 37 == 0) {
						streamId -= 0x1337;
						eventNumber = 1;
					} else
						eventNumber++;

					mtable.Add(streamId, eventNumber, logPosition);
				}

				using (PTable.FromMemtable(mtable, filename, skipIndexVerify: skipIndexVerify))
				{
				}

				File.Copy(filename, CopiedFileName);
			}
		}
	}
}
