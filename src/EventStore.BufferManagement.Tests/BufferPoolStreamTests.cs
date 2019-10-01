using System;
using System.IO;
using Xunit;

namespace EventStore.BufferManagement.Tests {
	public abstract class has_buffer_pool_fixture : has_buffer_manager_fixture {
		protected BufferPool BufferPool;

		public has_buffer_pool_fixture() {
			BufferPool = new BufferPool(10, BufferManager);
		}
	}

	public class when_insantiating_a_buffer_pool_stream : has_buffer_pool_fixture {
		[Fact]
		public void a_null_buffer_pool_throws_an_argumentnullexception() {
			Assert.Throws<ArgumentNullException>(() => new BufferPoolStream(null));
		}

		[Fact]
		public void the_internal_buffer_pool_is_set() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			Assert.Equal(BufferPool, stream.BufferPool);
		}
	}

	public class when_reading_from_the_stream : has_buffer_pool_fixture {
		[Fact]
		public void position_is_incremented() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			stream.Write(new byte[500], 0, 500);
			stream.Seek(0, SeekOrigin.Begin);
			Assert.Equal(0, stream.Position);
			stream.Read(new byte[50], 0, 50);
			Assert.Equal(50, stream.Position);
		}

		[Fact]
		public void a_read_past_the_end_of_the_stream_returns_zero() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			stream.Write(new byte[500], 0, 500);
			stream.Position = 0;
			int read = stream.Read(new byte[500], 0, 500);
			Assert.Equal(500, read);
			read = stream.Read(new byte[500], 0, 500);
			Assert.Equal(0, read);
		}

		[Fact]
		public void reading_from_the_stream_with_StreamCopyTo_returns_all_data() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			int size = 20123;
			stream.Write(new byte[size], 0, size);
			stream.Position = 0;

			var destination = new MemoryStream();
			stream.CopyTo(destination);
			Assert.Equal(destination.Length, size);
		}
	}

	public class when_writing_to_the_stream : has_buffer_pool_fixture {
		[Fact]
		public void position_is_incremented() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			stream.Write(new byte[500], 0, 500);
			Assert.Equal(500, stream.Position);
		}
	}

	public class when_seeking_in_the_stream : has_buffer_pool_fixture {
		[Fact]
		public void from_begin_sets_relative_to_beginning() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			stream.Write(new byte[500], 0, 500);
			stream.Seek(22, SeekOrigin.Begin);
			Assert.Equal(22, stream.Position);
		}

		[Fact]
		public void from_end_sets_relative_to_end() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			stream.Write(new byte[500], 0, 500);
			stream.Seek(-100, SeekOrigin.End);
			Assert.Equal(400, stream.Position);
		}

		[Fact]
		public void from_current_sets_relative_to_current() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			stream.Write(new byte[500], 0, 500);
			stream.Seek(-2, SeekOrigin.Current);
			stream.Seek(1, SeekOrigin.Current);
			Assert.Equal(499, stream.Position);
		}

		[Fact]
		public void a_negative_position_throws_an_argumentexception() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			Assert.Throws<ArgumentOutOfRangeException>(() => { stream.Seek(-1, SeekOrigin.Begin); });
		}

		[Fact]
		public void seeking_past_end_of_stream_throws_an_argumentexception() {
			BufferPoolStream stream = new BufferPoolStream(BufferPool);
			stream.Write(new byte[500], 0, 500);
			Assert.Throws<ArgumentOutOfRangeException>(() => { stream.Seek(501, SeekOrigin.Begin); });
		}
	}
}
