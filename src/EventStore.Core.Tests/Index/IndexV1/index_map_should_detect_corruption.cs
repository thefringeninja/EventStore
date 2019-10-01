using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Exceptions;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexV1 {
	public class index_map_should_detect_corruption {
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
		public void when_ptable_file_is_deleted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			fixture.PTable.MarkForDestruction();
			fixture.PTable = null;
			File.Delete(fixture.PTableFileName);

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_indexmap_file_does_not_have_md5_checksum(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var lines = File.ReadAllLines(fixture.IndexMapFileName);
			File.WriteAllLines(fixture.IndexMapFileName, lines.Skip(1));

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_indexmap_file_does_not_have_latest_commit_position(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var lines = File.ReadAllLines(fixture.IndexMapFileName);
			File.WriteAllLines(fixture.IndexMapFileName, lines.Where((x, i) => i != 1));

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_indexmap_file_exists_but_is_empty(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			File.WriteAllText(fixture.IndexMapFileName, "");

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_indexmap_file_is_garbage(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			File.WriteAllText(fixture.IndexMapFileName, "alkfjasd;lkf\nasdfasdf\n");

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_checkpoints_pair_is_corrupted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			using (var fs = File.Open(fixture.IndexMapFileName, FileMode.Open)) {
				fs.Position = 34;
				var b = (byte)fs.ReadByte();
				b ^= 1;
				fs.Position = 34;
				fs.WriteByte(b);
			}

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_ptable_line_is_missing_one_number(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var lines = File.ReadAllLines(fixture.IndexMapFileName);
			File.WriteAllLines(fixture.IndexMapFileName, new[] {lines[0], lines[1], string.Format("0,{0}", fixture.PTableFileName)});

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_ptable_line_constists_only_of_filename(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var lines = File.ReadAllLines(fixture.IndexMapFileName);
			File.WriteAllLines(fixture.IndexMapFileName, new[] {lines[0], lines[1], fixture.PTableFileName});

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_ptable_line_is_missing_filename(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			var lines = File.ReadAllLines(fixture.IndexMapFileName);
			File.WriteAllLines(fixture.IndexMapFileName, new[] {lines[0], lines[1], "0,0"});

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_indexmap_md5_checksum_is_corrupted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			using (var fs = File.Open(fixture.IndexMapFileName, FileMode.Open)) {
				var b = (byte)fs.ReadByte();
				b ^= 1; // swap single bit
				fs.Position = 0;
				fs.WriteByte(b);
			}

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_ptable_hash_is_corrupted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			fixture.PTable.Dispose();
			fixture.PTable = null;

			using (var fs = File.Open(fixture.PTableFileName, FileMode.Open)) {
				fs.Seek(-PTable.MD5Size, SeekOrigin.End);
				var b = (byte)fs.ReadByte();
				b ^= 1;
				fs.Seek(-PTable.MD5Size, SeekOrigin.End);
				fs.WriteByte(b);
			}

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_ptable_type_is_corrupted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			fixture.PTable.Dispose();
			fixture.PTable = null;

			using (var fs = File.Open(fixture.PTableFileName, FileMode.Open)) {
				fs.Seek(0, SeekOrigin.Begin);
				fs.WriteByte(123);
			}

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_ptable_header_is_corrupted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			fixture.PTable.Dispose();
			fixture.PTable = null;

			using (var fs = File.Open(fixture.PTableFileName, FileMode.Open)) {
				fs.Position = new Random().Next(0, PTableHeader.Size);
				var b = (byte)fs.ReadByte();
				b ^= 1;
				fs.Position -= 1;
				fs.WriteByte(b);
			}

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void when_ptable_data_is_corrupted(byte version, bool skipIndexVerify) {
			using var fixture = new Fixture(version, skipIndexVerify);
			fixture.PTable.Dispose();
			fixture.PTable = null;

			using (var fs = File.Open(fixture.PTableFileName, FileMode.Open)) {
				fs.Position = new Random().Next(PTableHeader.Size, (int)fs.Length);
				var b = (byte)fs.ReadByte();
				b ^= 1;
				fs.Position -= 1;
				fs.WriteByte(b);
			}

			Assert.Throws<CorruptIndexException>(() =>
				IndexMapTestFactory.FromFile(fixture.IndexMapFileName, maxTablesPerLevel: 2));
		}

		class Fixture : DirectoryFixture {
			public string IndexMapFileName;
			public string PTableFileName;
			public PTable PTable;
			private int _maxAutoMergeIndexLevel = 4;

			public Fixture(byte version, bool skipIndexVerify) {
				IndexMapFileName = GetFilePathFor("index.map");
				PTableFileName = GetFilePathFor("ptable");

				var indexMap = IndexMapTestFactory.FromFile(IndexMapFileName, maxTablesPerLevel: 2);
				var memtable = new HashListMemTable(version, maxSize: 10);
				memtable.Add(0, 0, 0);
				memtable.Add(1, 1, 100);
				PTable = PTable.FromMemtable(memtable, PTableFileName, skipIndexVerify: skipIndexVerify);

				indexMap = indexMap.AddPTable(PTable, 0, 0, (streamId, hash) => hash, _ => true,
					_ => new Tuple<string, bool>("", true), new GuidFilenameProvider(PathName), version,
					_maxAutoMergeIndexLevel, 0, skipIndexVerify: skipIndexVerify).MergedMap;
				indexMap.SaveToFile(IndexMapFileName);
			}

			public override void Dispose() {
				PTable?.MarkForDestruction();
				base.Dispose();
			}
		}
	}
}
