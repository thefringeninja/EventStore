using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Index;
using EventStore.Core.Util;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class saving_index_with_six_items_to_a_file {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV1};
			yield return new object[] {PTableVersions.IndexV2};
			yield return new object[] {PTableVersions.IndexV3};
			yield return new object[] {PTableVersions.IndexV4};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_file_exists(byte version) {
			using var fixture = new Fixture(version);
			Assert.True(File.Exists(fixture.FileName));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void the_file_contains_correct_data(byte version) {
			using var fixture = new Fixture(version);
			using (var fs = File.OpenRead(fixture.FileName))
			using (var reader = new StreamReader(fs)) {
				var text = reader.ReadToEnd();
				var lines = text.Replace("\r", "").Split('\n');

				fs.Position = 32;
				var md5 = MD5Hash.GetHashFor(fs);
				var md5String = BitConverter.ToString(md5).Replace("-", "");

				Assert.Equal(8, lines.Count());
				Assert.Equal(md5String, lines[0]);
				Assert.Equal(fixture.Map.Version.ToString(), lines[1]);
				Assert.Equal("7/11", lines[2]);
				Assert.Equal(int.MaxValue.ToString(), lines[3]);
				var name = new FileInfo(fixture.TableName).Name;
				Assert.Equal("0,0," + name, lines[4]);
				Assert.Equal("0,1," + name, lines[5]);
				Assert.Equal("1,0," + Path.GetFileName(fixture.MergeFile), lines[6]);
				Assert.Equal("", lines[7]);
			}
		}

		[Theory, MemberData(nameof(TestCases))]
		public void saved_file_could_be_read_correctly_and_without_errors(byte version) {
			using var fixture = new Fixture(version);
			var map = IndexMapTestFactory.FromFile(fixture.FileName);
			map.InOrder().ToList().ForEach(x => x.Dispose());

			Assert.Equal(7, map.PrepareCheckpoint);
			Assert.Equal(11, map.CommitCheckpoint);
		}

		class Fixture : DirectoryFixture {
			public readonly string FileName;
			public readonly string TableName;
			public readonly string MergeFile;
			public readonly IndexMap Map;
			private readonly MergeResult _result;
			private const int MaxAutoMergeIndexLevel = 4;

			public Fixture(byte version) {
				FileName = GetFilePathFor("indexfile");
				TableName = GetTempFilePath();
				MergeFile = GetFilePathFor("outfile");

				Map = IndexMapTestFactory.FromFile(FileName, maxTablesPerLevel: 4);
				var memtable = new HashListMemTable(version, maxSize: 10);
				memtable.Add(0, 2, 123);
				var table = PTable.FromMemtable(memtable, TableName);
				_result = Map.AddPTable(table, 0, 0, (streamId, hash) => hash, _ => true,
					_ => new Tuple<string, bool>("", true), new FakeFilenameProvider(MergeFile), version,
					MaxAutoMergeIndexLevel, 0);
				_result = _result.MergedMap.AddPTable(table, 0, 0, (streamId, hash) => hash, _ => true,
					_ => new Tuple<string, bool>("", true), new FakeFilenameProvider(MergeFile), version,
					MaxAutoMergeIndexLevel, 0);
				_result = _result.MergedMap.AddPTable(table, 0, 0, (streamId, hash) => hash, _ => true,
					_ => new Tuple<string, bool>("", true), new FakeFilenameProvider(MergeFile), version,
					MaxAutoMergeIndexLevel, 0);
				var merged = _result.MergedMap.AddPTable(table, 0, 0, (streamId, hash) => hash, _ => true,
					_ => new Tuple<string, bool>("", true), new FakeFilenameProvider(MergeFile), version,
					MaxAutoMergeIndexLevel, 0);
				_result = merged.MergedMap.AddPTable(table, 0, 0, (streamId, hash) => hash, _ => true,
					_ => new Tuple<string, bool>("", true), new FakeFilenameProvider(MergeFile), version,
					MaxAutoMergeIndexLevel, 0);
				_result = _result.MergedMap.AddPTable(table, 7, 11, (streamId, hash) => hash, _ => true,
					_ => new Tuple<string, bool>("", true), new FakeFilenameProvider(MergeFile), version,
					MaxAutoMergeIndexLevel, 0);
				_result.MergedMap.SaveToFile(FileName);

				table.Dispose();

				merged.MergedMap.InOrder().ToList().ForEach(x => x.Dispose());
				merged.ToDelete.ForEach(x => x.Dispose());

				_result.MergedMap.InOrder().ToList().ForEach(x => x.Dispose());
				_result.ToDelete.ForEach(x => x.Dispose());
			}
		}
	}
}
