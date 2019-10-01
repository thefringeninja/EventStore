using System;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.TransactionLog.Checkpoint;
using Xunit;
using EventStore.Core.Tests.Helpers;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_writing_a_file_checkpoint_to_a_writethroughfile : SpecificationWithFile {
		[PlatformFact("WINDOWS")]
		public void a_null_file_throws_argumentnullexception() {
			Assert.Throws<ArgumentNullException>(() => new FileCheckpoint(null));
		}

		[PlatformFact("WINDOWS")]
		public void name_is_set() {
			var checksum = new WriteThroughFileCheckpoint(HelperExtensions.GetFilePathFromAssembly("filename"), "test");
			Assert.Equal("test", checksum.Name);
			checksum.Close();
		}

		[PlatformFact("WINDOWS")]
		public void reading_off_same_instance_gives_most_up_to_date_info() {
			var checkSum = new WriteThroughFileCheckpoint(Filename);
			checkSum.Write(0xDEAD);
			checkSum.Flush();
			var read = checkSum.Read();
			checkSum.Close();
			Assert.Equal(0xDEAD, read);
		}

		[PlatformFact("WINDOWS")]
		public void can_read_existing_checksum() {
			var checksum = new WriteThroughFileCheckpoint(Filename);
			checksum.Write(0xDEAD);
			checksum.Close();
			checksum = new WriteThroughFileCheckpoint(Filename);
			var val = checksum.Read();
			checksum.Close();
			Assert.Equal(0xDEAD, val);
		}

		[PlatformFact("WINDOWS")]
		public async Task the_new_value_is_not_accessible_if_not_flushed_even_with_delay() {
			var checkSum = new WriteThroughFileCheckpoint(Filename);
			var readChecksum = new WriteThroughFileCheckpoint(Filename);
			checkSum.Write(1011);
			await Task.Delay(200);
			Assert.Equal(0, readChecksum.Read());
			checkSum.Close();
			readChecksum.Close();
		}

		[PlatformFact("WINDOWS")]
		public void the_new_value_is_accessible_after_flush() {
			var checkSum = new WriteThroughFileCheckpoint(Filename);
			var readChecksum = new WriteThroughFileCheckpoint(Filename);
			checkSum.Write(1011);
			checkSum.Flush();
			Assert.Equal(1011, readChecksum.Read());
			checkSum.Close();
			readChecksum.Close();
		}
	}
}
