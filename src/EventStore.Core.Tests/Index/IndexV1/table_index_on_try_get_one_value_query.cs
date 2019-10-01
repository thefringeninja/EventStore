using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.TransactionLog;
using Xunit;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class table_index_on_try_get_one_value_query {
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
			long position;
			Assert.False(fixture.TableIndex.TryGetOneValue("0xFEED", 0, out position));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void should_return_element_with_largest_position_when_hash_collisions(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			long position;
			Assert.True(fixture.TableIndex.TryGetOneValue("0xDEAD", 0, out position));
			Assert.Equal(0xFF10, position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void should_return_only_one_element_if_concurrency_duplicate_happens_on_range_query_as_well(byte version,
			bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var res = fixture.TableIndex.GetRange("0xADA", 0, 100).ToList();
			ulong hash = (ulong)fixture.LowHasher.Hash("0xADA");
			hash = version == PTableVersions.IndexV1 ? hash : hash << 32 | fixture.HighHasher.Hash("0xADA");
			Assert.Single(res);
			Assert.Equal(res[0].Stream, hash);
			Assert.Equal(res[0].Version, 0);
			Assert.Equal(res[0].Position, 0xFF00);
		}

		class Fixture : DirectoryFixture {
			public readonly TableIndex TableIndex;
			public readonly IHasher LowHasher;
			public readonly IHasher HighHasher;

			public Fixture(byte version, bool skipIndexVerify) {
				var fakeReader = new TFReaderLease(new FakeTfReader());
				LowHasher = new XXHashUnsafe();
				HighHasher = new Murmur3AUnsafe();
				TableIndex = new TableIndex(PathName, LowHasher, HighHasher,
					() => new HashListMemTable(version, maxSize: 10),
					() => fakeReader,
					version,
					5,
					maxSizeForMemory: 5,
					skipIndexVerify: skipIndexVerify);
				TableIndex.Initialize(long.MaxValue);

				TableIndex.Add(0, "0xDEAD", 0, 0xFF00);
				TableIndex.Add(0, "0xDEAD", 1, 0xFF01);

				TableIndex.Add(0, "0xBEEF", 0, 0xFF00);
				TableIndex.Add(0, "0xBEEF", 1, 0xFF01);

				TableIndex.Add(0, "0xABBA", 0, 0xFF00); // 1st ptable0

				TableIndex.Add(0, "0xABBA", 1, 0xFF01);
				TableIndex.Add(0, "0xABBA", 2, 0xFF02);
				TableIndex.Add(0, "0xABBA", 3, 0xFF03);

				TableIndex.Add(0, "0xADA", 0,
					0xFF00); // simulates duplicate due to concurrency in TableIndex (see memtable below)
				TableIndex.Add(0, "0xDEAD", 0, 0xFF10); // 2nd ptable0

				TableIndex.Add(0, "0xDEAD", 1, 0xFF11); // in memtable
				TableIndex.Add(0, "0xADA", 0, 0xFF00); // in memtable
			}


			public override void Dispose() {
				TableIndex.Close();
				base.Dispose();
			}
		}
	}
}
