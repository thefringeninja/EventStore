using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class adding_item_to_empty_index_map {
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
			Assert.Equal(7, fixture.Result.MergedMap.PrepareCheckpoint);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_commit_checkpoint_is_taken_from_the_latest_added_table(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Equal(11, fixture.Result.MergedMap.CommitCheckpoint);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void there_are_no_items_to_delete(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Empty(fixture.Result.ToDelete);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_merged_map_has_a_single_file(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Single(fixture.Result.MergedMap.GetAllFilenames());
			Assert.Equal(fixture.TableName, fixture.Result.MergedMap.GetAllFilenames().ToList()[0]);
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_original_map_did_not_change(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.Empty(fixture.Map.InOrder());
			Assert.Empty(fixture.Map.GetAllFilenames());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void a_merged_file_was_not_created(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			Assert.False(File.Exists(fixture.MergeFile));
		}

		class Fixture : DirectoryFixture {
			private readonly string _filename;
			public readonly IndexMap Map;
			public readonly string TableName;
			public readonly string MergeFile;
			public readonly MergeResult Result;
			private const int MaxAutoMergeIndexLevel = 4;

			public Fixture(byte version, bool skipIndexVerify) {
				_filename = GetTempFilePath();
				TableName = GetTempFilePath();
				MergeFile = GetFilePathFor("mergefile");

				Map = IndexMapTestFactory.FromFile(_filename);
				var memtable = new HashListMemTable(version, maxSize: 10);
				memtable.Add(0, 1, 0);
				var table = PTable.FromMemtable(memtable, TableName, skipIndexVerify: skipIndexVerify);
				Result = Map.AddPTable(table, 7, 11, (streamId, hash) => hash, _ => true,
					_ => new System.Tuple<string, bool>("", true), new FakeFilenameProvider(MergeFile), version,
					MaxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify);
				table.MarkForDestruction();
			}

			public override void Dispose() {
				File.Delete(_filename);
				File.Delete(MergeFile);
				File.Delete(TableName);
				
				base.Dispose();
			}
		}
	}
}
