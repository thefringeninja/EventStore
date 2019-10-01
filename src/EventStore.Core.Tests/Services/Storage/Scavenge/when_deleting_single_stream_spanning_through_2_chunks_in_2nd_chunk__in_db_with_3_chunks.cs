using System;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.Scavenge {
	public class
		when_deleting_single_stream_spanning_through_2_chunks_in_2nd_chunk_in_db_with_3_chunks : ReadIndexTestScenario {
		private EventRecord _event7;
		private PrepareLogRecord _event7prepare;
		private CommitLogRecord _event7commit;

		private EventRecord _event9;

		protected override void WriteTestScenario() {
			WriteSingleEvent("ES", 0, new string('.', 3000)); // chunk 1
			WriteSingleEvent("ES", 1, new string('.', 3000));
			WriteSingleEvent("ES", 2, new string('.', 3000));

			WriteSingleEvent("ES", 3, new string('.', 3000), retryOnFail: true); // chunk 2
			WriteSingleEvent("ES", 4, new string('.', 3000));

			_event7prepare = WriteDeletePrepare("ES");
			_event7commit = WriteDeleteCommit(_event7prepare);
			_event7 = new EventRecord(EventNumber.DeletedStream, _event7prepare);

			_event9 = WriteSingleEvent("ES2", 0, new string('.', 5000), retryOnFail: true); //chunk 3

			Scavenge(completeLast: false, mergeChunks: false);
		}

		[Fact]
		public void read_all_forward_does_not_return_scavenged_deleted_stream_events_and_return_remaining() {
			var events = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records.Select(r => r.Event).ToArray();
			Assert.Equal(2, events.Length);
			Assert.Equal(_event7, events[0]);
			Assert.Equal(_event9, events[1]);
		}

		[Fact]
		public void read_all_backward_does_not_return_scavenged_deleted_stream_events_and_return_remaining() {
			var events = ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100).Records.Select(r => r.Event)
				.ToArray();
			Assert.Equal(2, events.Length);
			Assert.Equal(_event7, events[1]);
			Assert.Equal(_event9, events[0]);
		}

		[Fact]
		public void read_all_backward_from_beginning_of_second_chunk_returns_no_records() {
			var pos = new TFPos(10000, 10000);
			var events = ReadIndex.ReadAllEventsBackward(pos, 100).Records.Select(r => r.Event).ToArray();
			Assert.Equal(0, events.Length);
		}

		[Fact]
		public void
			read_all_forward_from_beginning_of_2nd_chunk_with_max_2_record_returns_delete_record_and_record_from_3rd_chunk() {
			var events = ReadIndex.ReadAllEventsForward(new TFPos(10000, 10000), 2).Records.Select(r => r.Event)
				.ToArray();
			Assert.Equal(2, events.Length);
			Assert.Equal(_event7, events[0]);
			Assert.Equal(_event9, events[1]);
		}

		[Fact]
		public void read_all_forward_with_max_5_records_returns_2_records_from_2nd_chunk() {
			var events = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 5).Records.Select(r => r.Event).ToArray();
			Assert.Equal(2, events.Length);
			Assert.Equal(_event7, events[0]);
			Assert.Equal(_event9, events[1]);
		}

		[Fact]
		public void is_stream_deleted_returns_true() {
			Assert.True(ReadIndex.IsStreamDeleted("ES"));
		}

		[Fact]
		public void last_event_number_returns_stream_deleted() {
			Assert.Equal(EventNumber.DeletedStream, ReadIndex.GetStreamLastEventNumber("ES"));
		}

		[Fact]
		public void last_physical_record_from_scavenged_stream_should_remain() {
			// cannot use readIndex here as it doesn't return deleteTombstone

			var chunk = Db.Manager.GetChunk(1);
			var chunkPos = (int)(_event7prepare.LogPosition % Db.Config.ChunkSize);
			var res = chunk.TryReadAt(chunkPos);

			Assert.True(res.Success);
			Assert.Equal(_event7prepare, res.LogRecord);

			chunkPos = (int)(_event7commit.LogPosition % Db.Config.ChunkSize);
			res = chunk.TryReadAt(chunkPos);

			Assert.True(res.Success);
			Assert.Equal(_event7commit, res.LogRecord);
		}
	}
}
