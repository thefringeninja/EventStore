﻿using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;
using ReadStreamResult = EventStore.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace EventStore.Core.Tests.Services.Storage.Metastreams {
	public class when_having_deleted_stream_its_metastream_is_deleted_as_well : SimpleDbTestScenario {
		protected override DbResult CreateDb(TFChunkDbCreationHelper dbCreator) {
			return dbCreator.Chunk(Rec.Prepare(0, "test"),
					Rec.Commit(0, "test"),
					Rec.Prepare(1, "$$test", metadata: new StreamMetadata(2, null, null, null, null)),
					Rec.Commit(1, "$$test"),
					Rec.Delete(2, "test"),
					Rec.Commit(2, "test"))
				.CreateDb();
		}

		[Fact]
		public void the_stream_is_deleted() {
			Assert.True(ReadIndex.IsStreamDeleted("test"));
		}

		[Fact]
		public void the_metastream_is_deleted() {
			Assert.True(ReadIndex.IsStreamDeleted("$$test"));
		}

		[Fact]
		public void get_last_event_number_reports_deleted_metastream() {
			Assert.Equal(EventNumber.DeletedStream, ReadIndex.GetStreamLastEventNumber("$$test"));
		}

		[Fact]
		public void single_event_read_reports_deleted_metastream() {
			Assert.Equal(ReadEventResult.StreamDeleted, ReadIndex.ReadEvent("$$test", 0).Result);
		}

		[Fact]
		public void last_event_read_reports_deleted_metastream() {
			Assert.Equal(ReadEventResult.StreamDeleted, ReadIndex.ReadEvent("$$test", -1).Result);
		}

		[Fact]
		public void read_stream_events_forward_reports_deleted_metastream() {
			Assert.Equal(ReadStreamResult.StreamDeleted, ReadIndex.ReadStreamEventsForward("$$test", 0, 100).Result);
		}

		[Fact]
		public void read_stream_events_backward_reports_deleted_metastream() {
			Assert.Equal(ReadStreamResult.StreamDeleted,
				ReadIndex.ReadStreamEventsBackward("$$test", 0, 100).Result);
		}
	}
}
