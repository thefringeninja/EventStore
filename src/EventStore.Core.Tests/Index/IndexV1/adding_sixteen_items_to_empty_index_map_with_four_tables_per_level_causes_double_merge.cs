using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class
		adding_sixteen_items_to_empty_index_map_with_four_tables_per_level_causes_double_merge :
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
			using var fixture  = new Fixture(version, skipIndexVerify);
			Assert.Equal(1, fixture.Result.MergedMap.PrepareCheckpoint);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_commit_checkpoint_is_taken_from_the_latest_added_table(byte version, bool skipIndexVerify) {
			using var fixture  = new Fixture(version, skipIndexVerify);
			Assert.Equal(2, fixture.Result.MergedMap.CommitCheckpoint);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_eight_items_to_delete(byte version, bool skipIndexVerify) {
			using var fixture  = new Fixture(version, skipIndexVerify);
			Assert.Equal(8, fixture.Result.ToDelete.Count);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_merged_map_has_a_single_file(byte version, bool skipIndexVerify) {
			using var fixture  = new Fixture(version, skipIndexVerify);
			Assert.Equal(1, fixture.Result.MergedMap.GetAllFilenames().Count());
			Assert.Equal(fixture.FinalMergeFile2, fixture.Result.MergedMap.GetAllFilenames().ToList()[0]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_original_map_did_not_change(byte version, bool skipIndexVerify) {
			using var fixture  = new Fixture(version, skipIndexVerify);
			Assert.Equal(0, fixture.Map.InOrder().Count());
			Assert.Equal(0, fixture.Map.GetAllFilenames().Count());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void a_merged_file_was_created(byte version, bool skipIndexVerify) {
			using var fixture  = new Fixture(version, skipIndexVerify);
			Assert.True(File.Exists(fixture.FinalMergeFile2));
		}

		class Fixture : DirectoryFixture {
			private readonly string _filename;
			public readonly IndexMap Map;
			private readonly string _finalMergeFile;
			public string FinalMergeFile2;
			private const int MaxAutoMergeIndexLevel = 4;

			public MergeResult Result;

			public Fixture(byte version, bool skipIndexVerify) {
				_filename = GetTempFilePath();
				_finalMergeFile = GetTempFilePath();
				FinalMergeFile2 = GetTempFilePath();

				Map = IndexMapTestFactory.FromFile(_filename);
				var memtable = new HashListMemTable(version, maxSize: 10);
				memtable.Add(0, 1, 0);
				var guidFilename = new GuidFilenameProvider(PathName);
				Result = Map.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 0, 0,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true), guidFilename,
					version, MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
				Result = Result.MergedMap.AddPTable(
					PTable.FromMemtable(memtable, GetTempFilePath(), skipIndexVerify: skipIndexVerify), 1, 2,
					(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true),
					new FakeFilenameProvider(_finalMergeFile, FinalMergeFile2), version, 4, 0,
					skipIndexVerify: skipIndexVerify);
				Result.ToDelete.ForEach(x => x.MarkForDestruction());
			}

			public override void Dispose() {
				Result.MergedMap.InOrder().ToList().ForEach(x => x.MarkForDestruction());
				File.Delete(_filename);
				
				base.Dispose();
			}
		}
	}
}
