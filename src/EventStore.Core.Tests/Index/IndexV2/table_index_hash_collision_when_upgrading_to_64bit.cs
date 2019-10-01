using System.Linq;
using System.Threading;
using EventStore.Core.Index;
using EventStore.Core.TransactionLog;
using Xunit;
using EventStore.Core.Index.Hashes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.Tests.Index.IndexV2 {
	public class table_index_hash_collision_when_upgrading_to_64bit {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {0, 0};
			yield return new object[] {10, 0};
			yield return new object[] {0, 10};
			yield return new object[] {10, 10};
		}

		[Theory, MemberData(nameof(TestCases))]
		public async Task should_have_entries_in_sorted_order(int extraStreamHashesAtBeginning,
			int extraStreamHashesAtEnd) {
			using var fixture = new Fixture(extraStreamHashesAtBeginning, extraStreamHashesAtEnd);
			await Task.Delay(500);
			var streamId = "account--696193173";
			var result = fixture.TableIndex.GetRange(streamId, 0, 4).ToArray();
			var hash = (ulong)fixture.LowHasher.Hash(streamId) << 32 | fixture.HighHasher.Hash(streamId);

			Assert.Equal(5, result.Count());

			Assert.Equal(result[0].Stream, hash);
			Assert.Equal(result[0].Version, 4);
			Assert.Equal(result[0].Position, 10);

			Assert.Equal(result[1].Stream, hash);
			Assert.Equal(result[1].Version, 3);
			Assert.Equal(result[1].Position, 8);

			Assert.Equal(result[2].Stream, hash);
			Assert.Equal(result[2].Version, 2);
			Assert.Equal(result[2].Position, 6);

			Assert.Equal(result[3].Stream, hash);
			Assert.Equal(result[3].Version, 1);
			Assert.Equal(result[3].Position, 4);

			Assert.Equal(result[4].Stream, hash);
			Assert.Equal(result[4].Version, 0);
			Assert.Equal(result[4].Position, 2);

			streamId = "LPN-FC002_LPK51001";
			result = fixture.TableIndex.GetRange(streamId, 0, 4).ToArray();
			hash = (ulong)fixture.LowHasher.Hash(streamId) << 32 | fixture.HighHasher.Hash(streamId);

			Assert.Equal(5, result.Length);

			Assert.Equal(result[0].Stream, hash);
			Assert.Equal(result[0].Version, 4);
			Assert.Equal(result[0].Position, 9);

			Assert.Equal(result[1].Stream, hash);
			Assert.Equal(result[1].Version, 3);
			Assert.Equal(result[1].Position, 7);

			Assert.Equal(result[2].Stream, hash);
			Assert.Equal(result[2].Version, 2);
			Assert.Equal(result[2].Position, 5);

			Assert.Equal(result[3].Stream, hash);
			Assert.Equal(result[3].Version, 1);
			Assert.Equal(result[3].Position, 3);

			Assert.Equal(result[4].Stream, hash);
			Assert.Equal(result[4].Version, 0);
			Assert.Equal(result[4].Position, 1);
		}

		class Fixture : DirectoryFixture {
			public readonly TableIndex TableIndex;
			public readonly IHasher LowHasher;
			public readonly IHasher HighHasher;

			public Fixture(int extraStreamHashesAtBeginning, int extraStreamHashesAtEnd) {
				var fakeReader = new TFReaderLease(new FakeIndexReader());
				LowHasher = new XXHashUnsafe();
				HighHasher = new Murmur3AUnsafe();
				TableIndex = new TableIndex(PathName, LowHasher, HighHasher,
					() => new HashListMemTable(PTableVersions.IndexV1, maxSize: 5),
					() => fakeReader,
					PTableVersions.IndexV1,
					5,
					maxSizeForMemory: 5 + extraStreamHashesAtBeginning + extraStreamHashesAtEnd,
					maxTablesPerLevel: 2);
				TableIndex.Initialize(long.MaxValue);

				Assert.True(LowHasher.Hash("abcd") > LowHasher.Hash("LPN-FC002_LPK51001"));
				for (int i = 0; i < extraStreamHashesAtBeginning; i++) {
					TableIndex.Add(1, "abcd", i, i + 1);
				}

				Assert.True(LowHasher.Hash("wxyz") < LowHasher.Hash("LPN-FC002_LPK51001"));
				for (int i = 0; i < extraStreamHashesAtEnd; i++) {
					TableIndex.Add(1, "wxyz", i, i + 1);
				}

				TableIndex.Add(1, "LPN-FC002_LPK51001", 0, 1);
				TableIndex.Add(1, "account--696193173", 0, 2);
				TableIndex.Add(1, "LPN-FC002_LPK51001", 1, 3);
				TableIndex.Add(1, "account--696193173", 1, 4);
				TableIndex.Add(1, "LPN-FC002_LPK51001", 2, 5);

				TableIndex.Close(false);

				TableIndex = new TableIndex(PathName, LowHasher, HighHasher,
					() => new HashListMemTable(PTableVersions.IndexV2, maxSize: 5),
					() => fakeReader,
					PTableVersions.IndexV2,
					5,
					maxSizeForMemory: 5,
					maxTablesPerLevel: 2);
				TableIndex.Initialize(long.MaxValue);

				TableIndex.Add(1, "account--696193173", 2, 6);
				TableIndex.Add(1, "LPN-FC002_LPK51001", 3, 7);
				TableIndex.Add(1, "account--696193173", 3, 8);
				TableIndex.Add(1, "LPN-FC002_LPK51001", 4, 9);
				TableIndex.Add(1, "account--696193173", 4, 10);
			}

			public override void Dispose() {
				TableIndex.Close();
				base.Dispose();
			}
		}

		public class FakeIndexReader : ITransactionFileReader {
			public void Reposition(long position) {
				throw new NotImplementedException();
			}

			public SeqReadResult TryReadNext() {
				throw new NotImplementedException();
			}

			public SeqReadResult TryReadPrev() {
				throw new NotImplementedException();
			}

			public RecordReadResult TryReadAt(long position) {
				var record = (LogRecord)new PrepareLogRecord(position, Guid.NewGuid(), Guid.NewGuid(), 0, 0,
					position % 2 == 0 ? "account--696193173" : "LPN-FC002_LPK51001", -1, DateTime.UtcNow,
					PrepareFlags.None,
					"type", new byte[0], null);
				return new RecordReadResult(true, position + 1, record, 1);
			}

			public bool ExistsAt(long position) {
				return true;
			}
		}
	}
}
