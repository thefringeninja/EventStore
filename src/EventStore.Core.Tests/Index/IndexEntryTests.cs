using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index {
	public class IndexEntryTests {
		[Fact]
		public void key_is_made_of_stream_and_version() {
			var entry = new IndexEntryV1 {Stream = 0x01, Version = 0x12};
			Assert.Equal(0x0000000100000012UL, entry.Key);
		}

		[Fact]
		public void bytes_is_made_of_key_and_position() {
			unsafe {
				var entry = new IndexEntryV1 {Stream = 0x0101, Version = 0x1234, Position = 0xFFFF};
				Assert.Equal(0x34, entry.Bytes[0]);
				Assert.Equal(0x12, entry.Bytes[1]);
				Assert.Equal(0x00, entry.Bytes[2]);
				Assert.Equal(0x00, entry.Bytes[3]);
				Assert.Equal(0x01, entry.Bytes[4]);
				Assert.Equal(0x01, entry.Bytes[5]);
				Assert.Equal(0x00, entry.Bytes[6]);
				Assert.Equal(0x00, entry.Bytes[7]);
				Assert.Equal(0xFF, entry.Bytes[8]);
				Assert.Equal(0xFF, entry.Bytes[9]);
				Assert.Equal(0x00, entry.Bytes[10]);
				Assert.Equal(0x00, entry.Bytes[11]);
				Assert.Equal(0x00, entry.Bytes[12]);
				Assert.Equal(0x00, entry.Bytes[13]);
				Assert.Equal(0x00, entry.Bytes[14]);
				Assert.Equal(0x00, entry.Bytes[15]);
			}
		}
	}
}
