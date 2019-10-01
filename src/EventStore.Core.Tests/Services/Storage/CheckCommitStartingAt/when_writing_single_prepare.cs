using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.CheckCommitStartingAt {
	public class when_writing_single_prepare : ReadIndexTestScenario {
		private PrepareLogRecord _prepare;

		protected override void WriteTestScenario() {
			_prepare = WritePrepare("ES", -1);
		}

		[Fact]
		public void check_commmit_should_return_ok_decision() {
			var res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare.LogPosition,
				WriterCheckpoint.ReadNonFlushed());

			Assert.Equal(CommitDecision.Ok, res.Decision);
			Assert.Equal("ES", res.EventStreamId);
			Assert.Equal(-1, res.CurrentVersion);
			Assert.Equal(-1, res.StartEventNumber);
			Assert.Equal(-1, res.EndEventNumber);
		}
	}
}
