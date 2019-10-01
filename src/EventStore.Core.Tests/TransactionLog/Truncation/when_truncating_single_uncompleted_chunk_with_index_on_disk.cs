using EventStore.Core.Data;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation {
	public class when_truncating_single_uncompleted_chunk_with_index_on_disk : TruncateScenario {
		private EventRecord _event2;

		public when_truncating_single_uncompleted_chunk_with_index_on_disk()
			: base(maxEntriesInMemTable: 3) {
		}

		protected override void WriteTestScenario() {
			WriteSingleEvent("ES", 0, new string('.', 500));
			_event2 = WriteSingleEvent("ES", 1, new string('.', 500));
			WriteSingleEvent("ES", 2, new string('.', 500)); // index goes to disk
			WriteSingleEvent("ES", 3, new string('.', 500));

			TruncateCheckpoint = _event2.LogPosition;
		}

		[Fact]
		public void checksums_should_be_equal_to_ack_checksum() {
			Assert.Equal(TruncateCheckpoint, WriterCheckpoint.Read());
			Assert.Equal(TruncateCheckpoint, ChaserCheckpoint.Read());
		}
	}
}
