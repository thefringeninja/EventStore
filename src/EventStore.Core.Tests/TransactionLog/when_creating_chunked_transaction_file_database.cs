using System;
using EventStore.Core.TransactionLog.Chunks;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_creating_chunked_transaction_file_database {
		[Fact]
		public void a_null_config_throws_argument_null_exception() {
			Assert.Throws<ArgumentNullException>(() => new TFChunkDb(null));
		}
	}
}
