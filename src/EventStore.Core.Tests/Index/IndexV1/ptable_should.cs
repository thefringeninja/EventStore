using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class ptable_should {
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
		public void throw_argumentoutofrangeexception_on_range_query_when_provided_with_negative_start_version(
			byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.PTable.GetRange(0x0000, -1, long.MaxValue).ToArray());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_argumentoutofrangeexception_on_range_query_when_provided_with_negative_end_version(
			byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.PTable.GetRange(0x0000, 0, -1).ToArray());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_argumentoutofrangeexception_on_get_one_entry_query_when_provided_with_negative_version(
			byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			long pos;
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.PTable.TryGetOneValue(0x0000, -1, out pos));
		}

		class Fixture : FileFixture {
			public PTable PTable;

			public Fixture(byte version, bool skipIndexVerify) {
				var table = new HashListMemTable(version, maxSize: 10);
				table.Add(0x010100000000, 0x0001, 0x0001);
				PTable = PTable.FromMemtable(table, FileName, cacheDepth: 0, skipIndexVerify: skipIndexVerify);
			}

			public override void Dispose() {
				PTable.Dispose();
				base.Dispose();
			}
		}
	}
}
