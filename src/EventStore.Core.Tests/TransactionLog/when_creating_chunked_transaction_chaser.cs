using System;
using System.IO;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_creating_chunked_transaction_chaser : SpecificationWithDirectory {
		[Fact]
		public void a_null_file_config_throws_argument_null_exception() {
			Assert.Throws<ArgumentNullException>(
				() => new TFChunkChaser(null, new InMemoryCheckpoint(0), new InMemoryCheckpoint(0), false));
		}

		[Fact]
		public void a_null_writer_checksum_throws_argument_null_exception() {
			var db = new TFChunkDb(TFChunkHelper.CreateDbConfig(PathName, 0));
			Assert.Throws<ArgumentNullException>(() => new TFChunkChaser(db, null, new InMemoryCheckpoint(), false));
		}

		[Fact]
		public void a_null_chaser_checksum_throws_argument_null_exception() {
			var db = new TFChunkDb(TFChunkHelper.CreateDbConfig(PathName, 0));
			Assert.Throws<ArgumentNullException>(() => new TFChunkChaser(db, new InMemoryCheckpoint(), null, false));
		}
	}
}
