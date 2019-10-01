using System;
using System.IO;
using System.Threading.Tasks;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index.Scavenge {
	public class when_scavenging_an_index_fails : SpecificationWithDirectoryPerTestFixture {
		private PTable _oldTable;
		private string _expectedOutputFile;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			var table = new HashListMemTable(PTableVersions.IndexV4, maxSize: 20);
			table.Add(0x010100000000, 0, 1);
			table.Add(0x010200000000, 0, 2);
			table.Add(0x010300000000, 0, 3);
			table.Add(0x010300000000, 1, 4);
			_oldTable = PTable.FromMemtable(table, GetTempFilePath());

			long spaceSaved;
			Func<IndexEntry, bool> existsAt = x => { throw new Exception("Expected exception"); };
			Func<IndexEntry, Tuple<string, bool>> readRecord = x => { throw new Exception("Should not be called"); };
			Func<string, ulong, ulong> upgradeHash = (streamId, hash) => {
				throw new Exception("Should not be called");
			};

			_expectedOutputFile = GetTempFilePath();

			var ex = Assert.Throws<Exception>(
				() => PTable.Scavenged(_oldTable, _expectedOutputFile, upgradeHash, existsAt, readRecord,
					PTableVersions.IndexV4, out spaceSaved));
			Assert.Equal("Expected exception", ex.Message);
		}

		public override Task TestFixtureTearDown() {
			_oldTable.Dispose();

			return base.TestFixtureTearDown();
		}

		[Fact]
		public void the_output_file_is_deleted() {
			Assert.False(File.Exists(_expectedOutputFile));
		}
	}
}
