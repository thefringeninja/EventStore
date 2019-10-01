using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class
		adding_two_items_to_empty_index_map_with_two_tables_per_level_causes_merge :
			SpecificationWithDirectoryPerTestFixture {
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
		public void the_prepare_checkpoint_is_taken_from_the_latest_added_table(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Equal(100, fixture.Result.MergedMap.PrepareCheckpoint);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_commit_checkpoint_is_taken_from_the_latest_added_table(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Equal(400, fixture.Result.MergedMap.CommitCheckpoint);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_two_items_to_delete(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Equal(2, fixture.Result.ToDelete.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_merged_map_has_a_single_file(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Single(fixture.Result.MergedMap.GetAllFilenames());
			Assert.Equal(fixture.MergeFile, fixture.Result.MergedMap.GetAllFilenames().ToList()[0]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_original_map_did_not_change(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Empty(fixture.Map.InOrder());
			Assert.Empty(fixture.Map.GetAllFilenames());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void a_merged_file_was_created(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.True(File.Exists(fixture.MergeFile));
		}

		class Fixture : DirectoryFixture {
			private readonly string _filename;
			public readonly IndexMap Map;
			public readonly string MergeFile;
			public readonly MergeResult Result;
			private const int MaxAutoMergeIndexLevel = 4;

			public Fixture(byte version, bool skipIndexVerify) {
				_filename = GetTempFilePath();
				MergeFile = GetTempFilePath();

				Map = IndexMapTestFactory.FromFile(_filename, maxTablesPerLevel: 2);
				var memtable = new HashListMemTable(version, maxSize: 10);
				memtable.Add(0, 1, 0);

				Result = Map.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify),
					123, 321, (streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true),
					new GuidFilenameProvider(PathName), version, MaxAutoMergeIndexLevel, 0,
					skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify),
					100, 400, (streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true),
					new FakeFilenameProvider(MergeFile), version, MaxAutoMergeIndexLevel, 0,
					skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
			}

			public override void Dispose() {
				Result.MergedMap.InOrder().ToList().ForEach(x => x.MarkForDestruction());
				File.Delete(_filename);
				File.Delete(MergeFile);
				
				base.Dispose();
			}
		}
	}
}
