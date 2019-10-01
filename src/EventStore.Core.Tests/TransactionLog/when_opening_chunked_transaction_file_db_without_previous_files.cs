using System.IO;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_opening_chunked_transaction_file_db_without_previous_files : SpecificationWithDirectory {
		[Fact]
		public void with_a_writer_checksum_of_zero_the_first_chunk_is_created_with_correct_name_and_is_aligned() {
			var config = TFChunkHelper.CreateDbConfig(PathName, 0);
			var db = new TFChunkDb(config);
			db.Open();
			db.Dispose();

			Assert.Equal(1, Directory.GetFiles(PathName).Length);
			Assert.True(File.Exists(GetFilePathFor("chunk-000000.000000")));
			var fileInfo = new FileInfo(GetFilePathFor("chunk-000000.000000"));
			Assert.Equal(12288, fileInfo.Length);
		}
	}
}
