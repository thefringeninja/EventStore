using System.Collections.Generic;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV4 {
	public class
		when_merging_ptables_with_entries_to_nonexisting_record :
			IndexV1.when_merging_ptables_with_entries_to_nonexisting_record_in_newer_index_versions {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV4, false};
			yield return new object[] {PTableVersions.IndexV4, true};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_correct_midpoints_are_cached(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			PTable.Midpoint[] midpoints = fixture.NewTable.GetMidPoints();
			var requiredMidpoints = PTable.GetRequiredMidpointCountCached(fixture.NewTable.Count, version);

			Assert.Equal(requiredMidpoints, midpoints.LongLength);

			var position = 0;
			foreach (var item in fixture.NewTable.IterateAllInOrder()) {
				if (PTable.IsMidpointIndex(position, fixture.NewTable.Count, requiredMidpoints)) {
					Assert.Equal(item.Stream, midpoints[position].Key.Stream);
					Assert.Equal(item.Version, midpoints[position].Key.Version);
					Assert.Equal(position, midpoints[position].ItemIndex);
					position++;
				}
			}
		}
	}
}
