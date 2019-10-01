using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.CheckCommitStartingAt {
	public class when_writing_few_prepares_and_committing_one : ReadIndexTestScenario {
		private PrepareLogRecord _prepare0;
		private PrepareLogRecord _prepare1;
		private PrepareLogRecord _prepare2;

		protected override void WriteTestScenario() {
			_prepare0 = WritePrepare("ES", expectedVersion: -1);
			_prepare1 = WritePrepare("ES", expectedVersion: 0);
			_prepare2 = WritePrepare("ES", expectedVersion: 1);
			WriteCommit(_prepare0.LogPosition, "ES", eventNumber: 0);
		}

		[Fact]
		public void check_commmit_on_2nd_prepare_should_return_ok_decision() {
			var res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare1.LogPosition,
				WriterCheckpoint.ReadNonFlushed());

			Assert.Equal(CommitDecision.Ok, res.Decision);
			Assert.Equal("ES", res.EventStreamId);
			Assert.Equal(0, res.CurrentVersion);
			Assert.Equal(-1, res.StartEventNumber);
			Assert.Equal(-1, res.EndEventNumber);
		}

		[Fact]
		public void check_commmit_on_3rd_prepare_should_return_wrong_expected_version() {
			var res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare2.LogPosition,
				WriterCheckpoint.ReadNonFlushed());

			Assert.Equal(CommitDecision.WrongExpectedVersion, res.Decision);
			Assert.Equal("ES", res.EventStreamId);
			Assert.Equal(0, res.CurrentVersion);
			Assert.Equal(-1, res.StartEventNumber);
			Assert.Equal(-1, res.EndEventNumber);
		}
	}
}
