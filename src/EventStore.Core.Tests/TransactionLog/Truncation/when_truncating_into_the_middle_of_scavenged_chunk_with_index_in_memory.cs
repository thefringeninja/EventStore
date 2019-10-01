using System.IO;
using EventStore.Core.Data;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Truncation {
	public class when_truncating_into_the_middle_of_scavenged_chunk_with_index_in_memory : TruncateScenario {
		private string chunk0;
		private string chunk1;
		private string chunk2;
		private string chunk3;

		private EventRecord chunkEdge;

		protected override void WriteTestScenario() {
			WriteSingleEvent("ES1", 0, new string('.', 3000)); // chunk 0
			WriteSingleEvent("ES1", 1, new string('.', 3000));
			WriteSingleEvent("ES2", 0, new string('.', 3000));
			chunkEdge = WriteSingleEvent("ES1", 2, new string('.', 3000), retryOnFail: true); // chunk 1
			var ackRec = WriteSingleEvent("ES1", 3, new string('.', 3000));
			WriteSingleEvent("ES1", 4, new string('.', 3000));
			WriteSingleEvent("ES1", 5, new string('.', 3000), retryOnFail: true); // chunk 2
			WriteSingleEvent("ES1", 6, new string('.', 3000));
			WriteSingleEvent("ES1", 7, new string('.', 3000));
			WriteSingleEvent("ES1", 8, new string('.', 3000), retryOnFail: true); // chunk 3

			WriteDelete("ES1");
			Scavenge(completeLast: false, mergeChunks: false);

			TruncateCheckpoint = ackRec.LogPosition;
		}

		protected override void OnBeforeTruncating() {
			// scavenged chunk names
			// TODO MM: avoid this complexity - try scavenging exactly at where its invoked and not wait for readIndex to rebuild
			chunk0 = GetChunkName(0);
			chunk1 = GetChunkName(1);
			chunk2 = GetChunkName(2);
			chunk3 = GetChunkName(3);

			Assert.True(File.Exists(chunk0));
			Assert.True(File.Exists(chunk1));
			Assert.True(File.Exists(chunk2));
			Assert.True(File.Exists(chunk3));
		}

		private string GetChunkName(int chunkNumber) {
			var allVersions = Db.Config.FileNamingStrategy.GetAllVersionsFor(chunkNumber);
			Assert.Equal(1, allVersions.Length);
			return allVersions[0];
		}

		[Fact]
		public void checksums_should_be_equal_to_beginning_of_intersected_scavenged_chunk() {
			Assert.Equal(chunkEdge.TransactionPosition, WriterCheckpoint.Read());
			Assert.Equal(chunkEdge.TransactionPosition, ChaserCheckpoint.Read());
		}

		[Fact]
		public void truncated_chunks_should_be_deleted() {
			Assert.False(File.Exists(chunk2));
			Assert.False(File.Exists(chunk3));
		}

		[Fact]
		public void intersecting_chunk_should_be_deleted() {
			Assert.False(File.Exists(chunk1));
		}

		[Fact]
		public void untouched_chunk_should_survive() {
			var chunks = Db.Config.FileNamingStrategy.GetAllPresentFiles();
			Assert.Equal(1, chunks.Length);
			Assert.Equal(chunk0, GetChunkName(0));
		}
	}
}
