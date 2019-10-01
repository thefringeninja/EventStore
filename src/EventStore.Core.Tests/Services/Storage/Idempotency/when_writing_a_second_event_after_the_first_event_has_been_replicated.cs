using System;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.Idempotency {
	public class when_writing_a_second_event_after_the_first_event_has_been_replicated : WriteEventsToIndexScenario{
		private Guid _eventId = Guid.NewGuid();
        public override void WriteEvents()
        {
			var expectedEventNumber = -1;
			var transactionPosition = 1000;
			var prepares = CreatePrepareLogRecord("stream", expectedEventNumber, "type", _eventId, transactionPosition);
			var commit = CreateCommitLogRecord(transactionPosition + RecordOffset, transactionPosition, expectedEventNumber + 1);
			
			/*First write: committed to db and index*/
			WriteToDB(prepares);
			PreCommitToIndex(prepares);
			
			WriteToDB(commit);
			PreCommitToIndex(commit);
			
			CommitToIndex(prepares);
			CommitToIndex(commit);
        }

		[Fact]
		public void check_commit_with_same_expectedversion_should_return_idempotent_decision() {
			/*Second, idempotent write*/
			var commitCheckResult = _indexWriter.CheckCommit("stream", -1, new Guid[] { _eventId });
			Assert.Equal(CommitDecision.Idempotent, commitCheckResult.Decision);
		}

		[Fact]
		public void check_commit_with_expectedversion_any_should_return_idempotent_decision() {
			/*Second, idempotent write*/
			var commitCheckResult = _indexWriter.CheckCommit("stream", ExpectedVersion.Any, new Guid[] { _eventId });
			Assert.Equal(CommitDecision.Idempotent, commitCheckResult.Decision);
		}

		[Fact]
		public void check_commit_with_next_expectedversion_should_return_ok_decision() {
			var commitCheckResult = _indexWriter.CheckCommit("stream", 0, new Guid[] { _eventId });
			Assert.Equal(CommitDecision.Ok, commitCheckResult.Decision);
		}

		[Fact]
		public void check_commit_with_incorrect_expectedversion_should_return_wrongexpectedversion_decision() {
			var commitCheckResult = _indexWriter.CheckCommit("stream", 1, new Guid[] { _eventId });
			Assert.Equal(CommitDecision.WrongExpectedVersion, commitCheckResult.Decision);
		}
    }
}
