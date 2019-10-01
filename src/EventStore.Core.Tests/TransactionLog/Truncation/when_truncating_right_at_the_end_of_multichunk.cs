using System.IO;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.Tests.TransactionLog.Validation;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation {
	public class when_truncating_right_at_the_end_of_multichunk : SpecificationWithDirectoryPerTestFixture {
		private TFChunkDbConfig _config;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			_config = TFChunkHelper.CreateDbConfig(PathName, 13500, 5500, 5500, 11000, 1000);

			DbUtil.CreateMultiChunk(_config, 0, 2, GetFilePathFor("chunk-000000.000001"));
			DbUtil.CreateMultiChunk(_config, 0, 2, GetFilePathFor("chunk-000000.000002"));
			DbUtil.CreateMultiChunk(_config, 3, 10, GetFilePathFor("chunk-000003.000001"));
			DbUtil.CreateMultiChunk(_config, 3, 10, GetFilePathFor("chunk-000003.000002"));
			DbUtil.CreateMultiChunk(_config, 7, 8, GetFilePathFor("chunk-000007.000001"));
			DbUtil.CreateMultiChunk(_config, 11, 12, GetFilePathFor("chunk-000011.000001"));
			DbUtil.CreateOngoingChunk(_config, 13, GetFilePathFor("chunk-000013.000000"));

			var truncator = new TFChunkDbTruncator(_config);
			truncator.TruncateDb(_config.TruncateCheckpoint.ReadNonFlushed());
		}

		public override Task TestFixtureTearDown() {
			using (var db = new TFChunkDb(_config)) {
				db.Open(verifyHash: false);
			}

			Assert.True(File.Exists(GetFilePathFor("chunk-000000.000002")));
			Assert.True(File.Exists(GetFilePathFor("chunk-000003.000002")));
			Assert.True(File.Exists(GetFilePathFor("chunk-000011.000000")));
			Assert.Equal(3, Directory.GetFiles(PathName, "*").Length);

			return base.TestFixtureTearDown();
		}

		[Fact]
		public void writer_checkpoint_should_be_set_to_truncate_checkpoint() {
			Assert.Equal(11000, _config.WriterCheckpoint.Read());
			Assert.Equal(11000, _config.WriterCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void chaser_checkpoint_should_be_left_as_is_if_less_than_truncate_checkpoint() {
			Assert.Equal(5500, _config.ChaserCheckpoint.Read());
			Assert.Equal(5500, _config.ChaserCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void epoch_checkpoint_should_be_left_as_is_if_less_than_truncate_checkpoint() {
			Assert.Equal(5500, _config.EpochCheckpoint.Read());
			Assert.Equal(5500, _config.EpochCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void truncate_checkpoint_should_be_reset_after_truncation() {
			Assert.Equal(-1, _config.TruncateCheckpoint.Read());
			Assert.Equal(-1, _config.TruncateCheckpoint.ReadNonFlushed());
		}

		[Fact]
		public void every_chunk_that_end_by_the_truncation_checkpoint_is_preserved() {
			Assert.True(File.Exists(GetFilePathFor("chunk-000000.000001")));
			Assert.True(File.Exists(GetFilePathFor("chunk-000000.000002")));
			Assert.True(File.Exists(GetFilePathFor("chunk-000003.000001")));
			Assert.True(File.Exists(GetFilePathFor("chunk-000003.000002")));
			Assert.True(File.Exists(GetFilePathFor("chunk-000007.000001")));
			Assert.Equal(5, Directory.GetFiles(PathName, "*").Length);
		}
	}
}
