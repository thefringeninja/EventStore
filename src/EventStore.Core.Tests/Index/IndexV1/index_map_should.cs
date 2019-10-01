using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Exceptions;
using EventStore.Core.Index;
using EventStore.Core.Util;
using Xunit;
using EventStore.Common.Utils;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class index_map_should {
		public static IEnumerable<object[]> TestCases() {
			yield return new object[] {PTableVersions.IndexV1};
			yield return new object[] {PTableVersions.IndexV2};
			yield return new object[] {PTableVersions.IndexV3};
			yield return new object[] {PTableVersions.IndexV4};
		}

		[Theory, MemberData(nameof(TestCases))]
		public void not_allow_negative_prepare_checkpoint_when_adding_ptable(byte version) {
			using var fixture = new Fixture(version);
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.EmptyIndexMap.AddPTable(fixture.PTable, -1, 0,
				(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true),
				new GuidFilenameProvider(fixture.PathName), version, Fixture.MaxAutoMergeIndexLevel, 0));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void not_allow_negative_commit_checkpoint_when_adding_ptable(byte version) {
			using var fixture = new Fixture(version);
			Assert.Throws<ArgumentOutOfRangeException>(() => fixture.EmptyIndexMap.AddPTable(fixture.PTable, 0, -1,
				(streamId, hash) => hash, _ => true, _ => new System.Tuple<string, bool>("", true),
				new GuidFilenameProvider(fixture.PathName), version, Fixture.MaxAutoMergeIndexLevel, 0));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_corruptedindexexception_when_prepare_checkpoint_is_less_than_minus_one(byte version) {
			using var fixture = new Fixture(version);
			CreateArtificialIndexMapFile(fixture.IndexMapFileName, -2, 0, null);
			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void allow_prepare_checkpoint_equal_to_minus_one_if_no_ptables_are_in_index(byte version) {
			using var fixture = new Fixture(version);
			CreateArtificialIndexMapFile(fixture.IndexMapFileName, -1, 0, null);
			var indexMap = IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2);
			indexMap.InOrder().ToList().ForEach(x => x.Dispose());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void
			throw_corruptedindexexception_if_prepare_checkpoint_is_minus_one_and_there_are_ptables_in_indexmap(byte version) {
			using var fixture = new Fixture(version);
			CreateArtificialIndexMapFile(fixture.IndexMapFileName, -1, 0, fixture.PTableFileName);
			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void throw_corruptedindexexception_when_commit_checkpoint_is_less_than_minus_one(byte version) {
			using var fixture = new Fixture(version);
			CreateArtificialIndexMapFile(fixture.IndexMapFileName, 0, -2, null);
			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void allow_commit_checkpoint_equal_to_minus_one_if_no_ptables_are_in_index(byte version) {
			using var fixture = new Fixture(version);
			CreateArtificialIndexMapFile(fixture.IndexMapFileName, 0, -1, null);
			var indexMap = IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2);
			indexMap.InOrder().ToList().ForEach(x => x.Dispose());
		}

		[Theory, MemberData(nameof(TestCases))]
		public void
			throw_corruptedindexexception_if_commit_checkpoint_is_minus_one_and_there_are_ptables_in_indexmap(byte version) {
			using var fixture = new Fixture(version);
			CreateArtificialIndexMapFile(fixture.IndexMapFileName, 0, -1, fixture.PTableFileName);
			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		private void CreateArtificialIndexMapFile(string filePath, long prepareCheckpoint, long commitCheckpoint,
			string ptablePath) {
			using (var memStream = new MemoryStream())
			using (var memWriter = new StreamWriter(memStream)) {
				memWriter.WriteLine(new string('0', 32)); // pre-allocate space for MD5 hash
				memWriter.WriteLine("1");
				memWriter.WriteLine("{0}/{1}", prepareCheckpoint, commitCheckpoint);
				if (!string.IsNullOrWhiteSpace(ptablePath)) {
					memWriter.WriteLine("0,0,{0}", ptablePath);
				}

				memWriter.Flush();

				using (var f = File.OpenWrite(filePath))
				using (var fileWriter = new StreamWriter(f)) {
					memStream.Position = 0;
					memStream.CopyTo(f);

					memStream.Position = 32;
					var hash = MD5Hash.GetHashFor(memStream);
					f.Position = 0;
					for (int i = 0; i < hash.Length; ++i) {
						fileWriter.Write(hash[i].ToString("X2"));
					}

					fileWriter.WriteLine();
					fileWriter.Flush();
					f.FlushToDisk();
				}
			}
		}

		class Fixture : DirectoryFixture {
			public string IndexMapFileName;
			public string PTableFileName;
			public IndexMap EmptyIndexMap;
			public PTable PTable;
			public const int MaxAutoMergeIndexLevel = 4;

			public Fixture(byte version) {
				IndexMapFileName = GetFilePathFor("index.map");
				PTableFileName = GetFilePathFor("ptable");

				EmptyIndexMap = IndexMapTestFactory.FromFile(IndexMapFileName);

				var memTable = new HashListMemTable(version, maxSize: 10);
				memTable.Add(0, 1, 2);
				PTable = PTable.FromMemtable(memTable, PTableFileName);
			}

			public override void Dispose() {
				PTable?.MarkForDestruction();
				base.Dispose();
			}
		}
	}
}
