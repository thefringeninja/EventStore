using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.TransactionLog.Checkpoint;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class when_writing_a_memorymappedpoint_to_a_file : SpecificationWithFile {
		[PlatformFact("WINDOWS")]
		public void a_null_file_throws_argumentnullexception() {
			Assert.Throws<ArgumentNullException>(() => new MemoryMappedFileCheckpoint(null));
		}

		[PlatformFact("WINDOWS")]
		public void name_is_set() {
			var checksum = new MemoryMappedFileCheckpoint(Filename, "test", false);
			Assert.Equal("test", checksum.Name);
			checksum.Close();
		}

		[PlatformFact("WINDOWS")]
		public void reading_off_same_instance_gives_most_up_to_date_info() {
			var checkSum = new MemoryMappedFileCheckpoint(Filename);
			checkSum.Write(0xDEAD);
			checkSum.Flush();
			var read = checkSum.Read();
			checkSum.Close();
			Assert.Equal(0xDEAD, read);
		}

		[PlatformFact("WINDOWS")]
		public void can_read_existing_checksum() {
			var checksum = new MemoryMappedFileCheckpoint(Filename);
			checksum.Write(0xDEAD);
			checksum.Close();
			checksum = new MemoryMappedFileCheckpoint(Filename);
			var val = checksum.Read();
			checksum.Close();
			Assert.Equal(0xDEAD, val);
		}

		[PlatformFact("WINDOWS")]
		public async Task the_new_value_is_not_accessible_if_not_flushed_even_with_delay() {
			var checkSum = new MemoryMappedFileCheckpoint(Filename);
			var readChecksum = new MemoryMappedFileCheckpoint(Filename);
			checkSum.Write(1011);
			await Task.Delay(200);
			Assert.Equal(0, readChecksum.Read());
			readChecksum.Close();
		}

		[PlatformFact("WINDOWS")]
		public async Task the_new_value_is_accessible_after_flush() {
			var checkSum = new MemoryMappedFileCheckpoint(Filename);
			var readChecksum = new MemoryMappedFileCheckpoint(Filename);
			checkSum.Write(1011);
			checkSum.Flush();
			Assert.Equal(1011, readChecksum.Read());
			checkSum.Close();
			readChecksum.Close();
			await Task.Delay(100);
		}
	}
}
