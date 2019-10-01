using EventStore.Core.Exceptions;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_opening_tfchunk_from_non_existing_file : SpecificationWithFile {
		[Fact]
		public void it_should_throw_a_file_not_found_exception() {
			Assert.Throws<CorruptDatabaseException>(() => TFChunk.FromCompletedFile(Filename, verifyHash: true,
				unbufferedRead: false, initialReaderCount: 5, reduceFileCachePressure: false));
		}
	}
}
