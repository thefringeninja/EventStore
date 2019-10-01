using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Index;
using Xunit;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class table_index_should {
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
			Assert.Throws<ArgumentOutOfRangeException>(
				() => fixture.TableIndex.GetRange("0x0000", -1, long.MaxValue).ToArray());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_argumentoutofrangeexception_on_range_query_when_provided_with_negative_end_version(
			byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.TableIndex.GetRange("0x0000", 0, -1).ToArray());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_argumentoutofrangeexception_on_get_one_entry_query_when_provided_with_negative_version(
			byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			long pos;
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.TableIndex.TryGetOneValue("0x0000", -1, out pos));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_argumentoutofrangeexception_on_adding_entry_with_negative_commit_position(
			byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.TableIndex.Add(-1, "0x0000", 0, 0));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_argumentoutofrangeexception_on_adding_entry_with_negative_version(
			byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.TableIndex.Add(0, "0x0000", -1, 0));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_argumentoutofrangeexception_on_adding_entry_with_negative_position(
			byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.TableIndex.Add(0, "0x0000", 0, -1));
		}

		class Fixture : DirectoryFixture {
			public TableIndex TableIndex;

			public Fixture(byte version, bool skipIndexVerify) {
				var lowHasher = new XXHashUnsafe();
				var highHasher = new Murmur3AUnsafe();
				TableIndex = new TableIndex(PathName, lowHasher, highHasher,
					() => new HashListMemTable(version, maxSize: 20),
					() => { throw new InvalidOperationException(); },
					version,
					5,
					maxSizeForMemory: 10,
					skipIndexVerify: skipIndexVerify);
				TableIndex.Initialize(long.MaxValue);
			}

			public override void Dispose() {
				TableIndex.Close();
				base.Dispose();
			}
		}
	}
}
