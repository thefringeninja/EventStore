using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.Tests.TransactionLog.Validation;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation {
	public class when_truncating_into_the_middle_of_ongoing_chunk : SpecificationWithDirectoryPerTestFixture {
		private TFChunkDbConfig _config;
		private byte[] _file1Contents;
		private byte[] _file2Contents;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			_config = TFChunkHelper.CreateDbConfig(PathName, 1711, 5500, 5500, 1111, 1000);

			var rnd = new Random();
			_file1Contents = new byte[_config.ChunkSize];
			_file2Contents = new byte[_config.ChunkSize];
			rnd.NextBytes(_file1Contents);
			rnd.NextBytes(_file2Contents);

			DbUtil.CreateSingleChunk(_config, 0, GetFilePathFor("chunk-000000.000001"), contents: _file1Contents);
			DbUtil.CreateOngoingChunk(_config, 1, GetFilePathFor("chunk-000001.000002"), contents: _file2Contents);

			var truncator = new TFChunkDbTruncator(_config);
			truncator.TruncateDb(_config.TruncateCheckpoint.ReadNonFlushed());
		}

		public override Task TestFixtureTearDown() {
			using (var db = new TFChunkDb(_config)) {
				db.Open(verifyHash: false);
			}

			Assert.True(File.Exists(GetFilePathFor("chunk-000000.000001")));
			Assert.True(File.Exists(GetFilePathFor("chunk-000001.000002")));
			Assert.Equal(2, Directory.GetFiles(PathName, "*").Length);

			return base.TestFixtureTearDown();
		}

		[Fact]
		public void writer_checkpoint_should_be_set_to_exactly_truncation_checkpoint() {
			Assert.Equal(1111, _config.WriterCheckpoint.Read());
			Assert.Equal(1111, _config.WriterCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void chaser_checkpoint_should_be_adjusted_if_less_than_actual_truncate_checkpoint() {
			Assert.Equal(1111, _config.ChaserCheckpoint.Read());
			Assert.Equal(1111, _config.ChaserCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void epoch_checkpoint_should_be_reset_if_less_than_actual_truncate_checkpoint() {
			Assert.Equal(-1, _config.EpochCheckpoint.Read());
			Assert.Equal(-1, _config.EpochCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void truncate_checkpoint_should_be_reset_after_truncation() {
			Assert.Equal(-1, _config.TruncateCheckpoint.Read());
			Assert.Equal(-1, _config.TruncateCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void all_chunks_are_preserved() {
			Assert.True(File.Exists(GetFilePathFor("chunk-000000.000001")));
			Assert.True(File.Exists(GetFilePathFor("chunk-000001.000002")));
			Assert.Equal(2, Directory.GetFiles(PathName, "*").Length);
		}

		[Fact]
		public void contents_of_first_chunk_should_be_untouched() {
			var contents = new byte[_config.ChunkSize];
			using (var fs = File.OpenRead(GetFilePathFor("chunk-000000.000001"))) {
				fs.Position = ChunkHeader.Size;
				fs.Read(contents, 0, contents.Length);
				Assert.Equal(_file1Contents, contents);
			}
		}

		[Fact]
		public void ongoing_chunk_should_have_full_size_and_filled_with_zeros_after_writer_checkpoint() {
			var fileInfo = new FileInfo(GetFilePathFor("chunk-000001.000002"));
			Assert.Equal(ChunkHeader.Size + 1000 + ChunkFooter.Size, fileInfo.Length);

			using (var fs = File.OpenRead(fileInfo.FullName)) {
				var leftDataSize = (int)(_config.WriterCheckpoint.Read() % _config.ChunkSize);
				var leftData = new byte[leftDataSize];
				var shouldBeZeros = new byte[_config.ChunkSize - leftDataSize + ChunkFooter.Size];

				fs.Position = ChunkHeader.Size;
				fs.Read(leftData, 0, leftData.Length);

				var shouldBeLeft = new byte[leftDataSize];
				Buffer.BlockCopy(_file2Contents, 0, shouldBeLeft, 0, leftDataSize);
				Assert.Equal(shouldBeLeft, leftData);

				fs.Position = ChunkHeader.Size + _config.WriterCheckpoint.Read() % _config.ChunkSize;
				fs.Read(shouldBeZeros, 0, shouldBeZeros.Length);

				Assert.True(shouldBeZeros.All(x => x == 0), "Chunk is not zeroed!");
			}
		}
	}
}
