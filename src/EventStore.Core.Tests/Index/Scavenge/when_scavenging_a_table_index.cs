using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using EventStore.Core.Index;
using EventStore.Core.Index.Hashes;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog;
using Xunit;

namespace EventStore.Core.Tests.Index.Scavenge {
	public class when_scavenging_a_table_index {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {false};
			yield return new object[] {true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void should_have_logged_each_index_table(bool skipIndexVerify) {
			var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(3, fixture.Log.ScavengedIndices.Count);
			Assert.True(fixture.Log.ScavengedIndices[0].Scavenged);
			;
			Assert.Null(fixture.Log.ScavengedIndices[0].Error);
			Assert.Equal(1, fixture.Log.ScavengedIndices[0].EntriesDeleted);
			Assert.True(fixture.Log.ScavengedIndices[1].Scavenged);
			;
			Assert.Null(fixture.Log.ScavengedIndices[1].Error);
			Assert.Equal(2, fixture.Log.ScavengedIndices[1].EntriesDeleted);
			Assert.False(fixture.Log.ScavengedIndices[2].Scavenged);
			Assert.Empty(fixture.Log.ScavengedIndices[2].Error);
			Assert.Equal(0, fixture.Log.ScavengedIndices[2].EntriesDeleted);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void should_have_entries_in_sorted_order(bool skipIndexVerify) {
			var fixture = new Fixture(skipIndexVerify);
			var streamId = "testStream-1";
			var result = fixture.TableIndex.GetRange(streamId, 0, 5).ToArray();
			var hash = (ulong)fixture.LowHasher.Hash(streamId) << 32 | fixture.HighHasher.Hash(streamId);

			Assert.Equal(3, result.Length);

			Assert.Equal(hash, result[0].Stream);
			Assert.Equal(4, result[0].Version);
			Assert.Equal(400, result[0].Position);

			Assert.Equal(hash, result[1].Stream);
			Assert.Equal(1, result[1].Version);
			Assert.Equal(100, result[1].Position);

			Assert.Equal(hash, result[2].Stream);
			Assert.Equal(0, result[2].Version);
			Assert.Equal(0, result[2].Position);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void old_index_tables_are_deleted(bool skipIndexVerify) {
			var fixture = new Fixture(skipIndexVerify);
			Assert.Equal(4, Directory.EnumerateFiles(fixture.PathName).Count());
		}

		class Fixture : DirectoryFixture {
			public readonly TableIndex TableIndex;
			public readonly IHasher LowHasher;
			public readonly IHasher HighHasher;
			public readonly FakeTFScavengerLog Log;
			private static readonly long[] Deleted = {200, 300, 500};

			public Fixture(bool skipIndexVerify) {
				var fakeReader = new TFReaderLease(new FakeIndexReader(l => !Deleted.Contains(l)));

				LowHasher = new XXHashUnsafe();
				HighHasher = new Murmur3AUnsafe();
				TableIndex = new TableIndex(PathName, LowHasher, HighHasher,
					() => new HashListMemTable(PTableVersions.IndexV4, maxSize: 5),
					() => fakeReader,
					PTableVersions.IndexV4,
					5,
					maxSizeForMemory: 2,
					maxTablesPerLevel: 5, skipIndexVerify: skipIndexVerify);
				TableIndex.Initialize(long.MaxValue);


				TableIndex.Add(1, "testStream-1", 0, 0);
				TableIndex.Add(1, "testStream-1", 1, 100);
				TableIndex.Add(1, "testStream-1", 2, 200);
				TableIndex.Add(1, "testStream-1", 3, 300);
				TableIndex.Add(1, "testStream-1", 4, 400);
				TableIndex.Add(1, "testStream-1", 5, 500);

				Log = new FakeTFScavengerLog();
				TableIndex.Scavenge(Log, CancellationToken.None);

				// Check it's loadable.
				TableIndex.Close(false);

				TableIndex = new TableIndex(PathName, LowHasher, HighHasher,
					() => new HashListMemTable(PTableVersions.IndexV4, maxSize: 5),
					() => fakeReader,
					PTableVersions.IndexV4,
					5,
					maxSizeForMemory: 2,
					maxTablesPerLevel: 5);

				TableIndex.Initialize(long.MaxValue);
			}

			public override void Dispose() {
				TableIndex.Close();
				
				base.Dispose();
			}
		}
	}
}
