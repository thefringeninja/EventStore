using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Index;
using Xunit;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class table_index_on_range_query {
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
		public void should_return_empty_collection_when_stream_is_not_in_db(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var res = fixture.TableIndex.GetRange("0xFEED", 0, 100);
			Assert.Empty(res);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void should_return_all_applicable_elements_in_correct_order(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var res = fixture.TableIndex.GetRange("0xJEEP", 0, 100).ToList();
			ulong hash = (ulong)fixture.LowHasher.Hash("0xJEEP");
			hash = version == PTableVersions.IndexV1 ? hash : hash << 32 | fixture.HighHasher.Hash("0xJEEP");
			Assert.Equal(2, res.Count());
			Assert.Equal(hash, res[0].Stream);
			Assert.Equal(1, res[0].Version);
			Assert.Equal(0xFF01, res[0].Position);
			Assert.Equal(hash, res[1].Stream);
			Assert.Equal(0, res[1].Version);
			Assert.Equal(0xFF00, res[1].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void should_return_all_elements_with_hash_collisions_in_correct_order(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var res = fixture.TableIndex.GetRange("0xDEAD", 0, 100).ToList();
			ulong hash = (ulong)fixture.LowHasher.Hash("0xDEAD");
			hash = version == PTableVersions.IndexV1 ? hash : hash << 32 | fixture.HighHasher.Hash("0xDEAD");
			Assert.Equal(4, res.Count());
			Assert.Equal(hash, res[0].Stream);
			Assert.Equal(1, res[0].Version);
			Assert.Equal(0xFF11, res[0].Position);

			Assert.Equal(hash, res[1].Stream);
			Assert.Equal(1, res[1].Version);
			Assert.Equal(0xFF01, res[1].Position);

			Assert.Equal(hash, res[2].Stream);
			Assert.Equal(0, res[2].Version);
			Assert.Equal(0xFF10, res[2].Position);

			Assert.Equal(hash, res[3].Stream);
			Assert.Equal(0, res[3].Version);
			Assert.Equal(0xFF00, res[3].Position);
		}

		class Fixture : DirectoryFixture {
			public readonly TableIndex TableIndex;
			public readonly IHasher LowHasher;
			public readonly IHasher HighHasher;

			public Fixture(byte version, bool skipIndexVerify) {
				LowHasher = new XXHashUnsafe();
				HighHasher = new Murmur3AUnsafe();
				TableIndex = new TableIndex(PathName, LowHasher, HighHasher,
					() => new HashListMemTable(version: version, maxSize: 40),
					() => { throw new InvalidOperationException(); },
					version,
					5,
					maxSizeForMemory: 20,
					skipIndexVerify: skipIndexVerify);
				TableIndex.Initialize(long.MaxValue);

				TableIndex.Add(0, "0xDEAD", 0, 0xFF00);
				TableIndex.Add(0, "0xDEAD", 1, 0xFF01);

				TableIndex.Add(0, "0xJEEP", 0, 0xFF00);
				TableIndex.Add(0, "0xJEEP", 1, 0xFF01);

				TableIndex.Add(0, "0xABBA", 0, 0xFF00);
				TableIndex.Add(0, "0xABBA", 1, 0xFF01);
				TableIndex.Add(0, "0xABBA", 2, 0xFF02);
				TableIndex.Add(0, "0xABBA", 3, 0xFF03);

				TableIndex.Add(0, "0xDEAD", 0, 0xFF10);
				TableIndex.Add(0, "0xDEAD", 1, 0xFF11);

				TableIndex.Add(0, "0xADA", 0, 0xFF00);
			}

			public override void Dispose() {
				TableIndex.Close();
				base.Dispose();
			}
		}
	}
}
