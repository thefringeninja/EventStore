using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EventStore.Core.Index;
using EventStore.Core.Util;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class saving_index_with_single_item_to_a_file {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV1};
			yield return new object[] {PTableVersions.IndexV2};
			yield return new object[] {PTableVersions.IndexV3};
			yield return new object[] {PTableVersions.IndexV4};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_file_exists(byte version) {
			using var fixture = new Fixture(version);
			Assert.True(File.Exists(fixture.Filename));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_file_contains_correct_dat(byte version) {
			using var fixture = new Fixture(version);
			using (var fs = File.OpenRead(fixture.Filename))
			using (var reader = new StreamReader(fs)) {
				var text = reader.ReadToEnd();
				var lines = text.Replace("\r", "").Split('\n');

				fs.Position = 32;
				var md5 = MD5Hash.GetHashFor(fs);
				var md5String = BitConverter.ToString(md5).Replace("-", "");

				Assert.Equal(6, lines.Count());
				Assert.Equal(md5String, lines[0]);
				Assert.Equal(fixture.Map.Version.ToString(), lines[1]);
				Assert.Equal("7/11", lines[2]);
				Assert.Equal(Fixture.MaxAutoMergeIndexLevel.ToString(), lines[3]);
				Assert.Equal("0,0," + Path.GetFileName(fixture.TableName), lines[4]);
				Assert.Equal("", lines[5]);
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void saved_file_could_be_read_correctly_and_without_errors(byte version) {
			using var fixture = new Fixture(version);
			var map = IndexMapTestFactory.FromFile(fixture.Filename, maxAutoMergeLevel: Fixture.MaxAutoMergeIndexLevel);
			map.InOrder().ToList().ForEach(x => x.Dispose());

			Assert.Equal(7, map.PrepareCheckpoint);
			Assert.Equal(11, map.CommitCheckpoint);
		}

		class Fixture : DirectoryFixture {
			public readonly string Filename;
			public readonly IndexMap Map;
			public readonly string TableName;
			private readonly string _mergeFile;
			private readonly MergeResult _result;
			public const int MaxAutoMergeIndexLevel = 4;

			public Fixture(byte version) {
				Filename = GetFilePathFor("indexfile");
				TableName = GetTempFilePath();
				_mergeFile = GetFilePathFor("outputfile");

				Map = IndexMapTestFactory.FromFile(Filename, maxAutoMergeLevel: MaxAutoMergeIndexLevel);
				var memtable = new HashListMemTable(version, maxSize: 10);
				memtable.Add(0, 2, 7);
				var table = PTable.FromMemtable(memtable, TableName);
				_result = Map.AddPTable(table, 7, 11, (streamId, hash) => hash, _ => true,
					_ => new Tuple<string, bool>("", true), new FakeFilenameProvider(_mergeFile), version, 0);
				_result.MergedMap.SaveToFile(Filename);
				_result.ToDelete.ForEach(x => x.Dispose());
				_result.MergedMap.InOrder().ToList().ForEach(x => x.Dispose());
				table.Dispose();
			}

			public override void Dispose() {
				_result.ToDelete.ForEach(x => x.MarkForDestruction());
				_result.MergedMap.InOrder().ToList().ForEach(x => x.MarkForDestruction());
				_result.MergedMap.InOrder().ToList().ForEach(x => x.WaitForDisposal(1000));
				base.Dispose();
			}
		}
	}
}
