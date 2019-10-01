using System;
using System.Collections.Generic;
using EventStore.Common.Log;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class ptable_midpoint_cache_should : SpecificationWithDirectory {
		private static readonly ILogger Log = LogManager.GetLoggerFor<ptable_midpoint_cache_should>();

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

		private void construct_valid_cache_for_any_combination_of_params(int maxIndexEntries, byte pTableVersion,
			bool skipIndexVerify) {
			var rnd = new Random(123987);
			for (int count = 0; count < maxIndexEntries; ++count) {
				for (int depth = 0; depth < 15; ++depth) {
					PTable ptable = null;
					try {
						Log.Trace("Creating PTable with count {0}, depth {1}", count, depth);
						ptable = ConstructPTable(
							GetFilePathFor(string.Format("{0}-{1}-indexv{2}.ptable", count, depth, pTableVersion)),
							count, rnd, depth, pTableVersion, skipIndexVerify);
						ValidateCache(ptable.GetMidPoints(), count, depth);
					} finally {
						ptable?.Dispose();
					}
				}
			}
		}

		private PTable ConstructPTable(string file, int count, Random rnd, int depth, byte pTableVersion,
			bool skipIndexVerify) {
			var memTable = new HashListMemTable(pTableVersion, 20000);
			for (int i = 0; i < count; ++i) {
				memTable.Add((uint)rnd.Next(), rnd.Next(0, 1 << 20), Math.Abs(rnd.Next() * rnd.Next()));
			}

			var ptable = PTable.FromMemtable(memTable, file, depth, skipIndexVerify: skipIndexVerify);
			return ptable;
		}

		private void ValidateCache(PTable.Midpoint[] cache, int count, int depth) {
			if (count == 0 || depth == 0) {
				Assert.Null(cache);
				return;
			}

			if (count == 1) {
				Assert.NotNull(cache);
				Assert.Equal(2, cache.Length);
				Assert.Equal(0, cache[0].ItemIndex);
				Assert.Equal(0, cache[1].ItemIndex);
				return;
			}

			Assert.NotNull(cache);
			Assert.Equal(Math.Min(count, 1 << depth), cache.Length);

			Assert.Equal(0, cache[0].ItemIndex);
			for (int i = 1; i < cache.Length; ++i) {
				Assert.True(cache[i - 1].Key.GreaterEqualsThan(cache[i].Key));
				Assert.True(cache[i - 1].ItemIndex < cache[i].ItemIndex);
			}

			Assert.Equal(count - 1, cache[cache.Length - 1].ItemIndex);
		}

		[Theory(Skip = "Veerrrryyy long running :)"), MemberData(nameof(TestCases)), Trait("Category", "LongRunning")]
		public void construct_valid_cache_for_any_combination_of_params_large(byte pTableVersion,
			bool skipIndexVerify) {
			construct_valid_cache_for_any_combination_of_params(4096, pTableVersion, skipIndexVerify);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void construct_valid_cache_for_any_combination_of_params_small(byte pTableVersion,
			bool skipIndexVerify) {
			construct_valid_cache_for_any_combination_of_params(20, pTableVersion, skipIndexVerify);
		}
	}
}
