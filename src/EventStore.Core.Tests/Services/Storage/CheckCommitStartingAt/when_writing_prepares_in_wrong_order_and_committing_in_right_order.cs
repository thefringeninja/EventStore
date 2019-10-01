using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.CheckCommitStartingAt {
	public class when_writing_prepares_in_wrong_order_and_committing_in_right_order : ReadIndexTestScenario {
		private PrepareLogRecord _prepare0;
		private PrepareLogRecord _prepare1;
		private PrepareLogRecord _prepare2;
		private PrepareLogRecord _prepare3;
		private PrepareLogRecord _prepare4;

		protected override void WriteTestScenario() {
			_prepare0 = WritePrepare("ES", expectedVersion: -1);
			_prepare1 = WritePrepare("ES", expectedVersion: 2);
			_prepare2 = WritePrepare("ES", expectedVersion: 0);
			_prepare3 = WritePrepare("ES", expectedVersion: 1);
			_prepare4 = WritePrepare("ES", expectedVersion: 3);
			WriteCommit(_prepare0.LogPosition, "ES", eventNumber: 0);
			WriteCommit(_prepare2.LogPosition, "ES", eventNumber: 1);
			WriteCommit(_prepare3.LogPosition, "ES", eventNumber: 2);
		}

		[Fact]
		public void check_commmit_on_expected_prepare_should_return_ok_decision() {
			var res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare1.LogPosition,
				WriterCheckpoint.ReadNonFlushed());

			Assert.Equal(CommitDecision.Ok, res.Decision);
			Assert.Equal("ES", res.EventStreamId);
			Assert.Equal(2, res.CurrentVersion);
			Assert.Equal(-1, res.StartEventNumber);
			Assert.Equal(-1, res.EndEventNumber);
		}

		[Fact]
		public void check_commmit_on_not_expected_prepare_should_return_wrong_expected_version() {
			var res = ReadIndex.IndexWriter.CheckCommitStartingAt(_prepare4.LogPosition,
				WriterCheckpoint.ReadNonFlushed());

			Assert.Equal(CommitDecision.WrongExpectedVersion, res.Decision);
			Assert.Equal("ES", res.EventStreamId);
			Assert.Equal(2, res.CurrentVersion);
			Assert.Equal(-1, res.StartEventNumber);
			Assert.Equal(-1, res.EndEventNumber);
		}
	}
}
