using System;
using System.IO;
using EventStore.Core.TransactionLog.Unbuffered;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog.Unbuffered {
	public class UnbufferedTests : SpecificationWithDirectory {
		[Fact]
		public void when_resizing_a_file() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			stream.SetLength(4096 * 1024);
			stream.Close();
			Assert.Equal(4096 * 1024, new FileInfo(filename).Length);
		}

		[Fact]
		public void when_expanding_an_aligned_file_by_one_page() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(0, SeekOrigin.End);

			Assert.Equal(initialFileSize, stream.Position); //verify position
			stream.SetLength(initialFileSize + 4096); //expand file by 4KB
			Assert.Equal(initialFileSize, stream.Position); //position should not change
			
			stream.Close();

			Assert.Equal(initialFileSize + 4096, new FileInfo(filename).Length); //file size should increase by 4KB
		}

		[Fact]
		public void when_expanding_an_aligned_file_by_one_byte_less_than_one_page() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(0, SeekOrigin.End);

			Assert.Equal(initialFileSize, stream.Position); //verify position
			stream.SetLength(initialFileSize + 4095); //expand file by 4KB - 1
			Assert.Equal(initialFileSize, stream.Position); //position should not change
			
			stream.Close();

			Assert.Equal(initialFileSize + 4096, new FileInfo(filename).Length); //file size should increase by 4KB
		}

		[Fact]
		public void when_expanding_an_aligned_file_by_one_byte_more_than_one_page() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(0, SeekOrigin.End);

			Assert.Equal(initialFileSize, stream.Position); //verify position
			stream.SetLength(initialFileSize + 4097); //expand file by 4KB + 1
			Assert.Equal(initialFileSize, stream.Position); //position should not change
			
			stream.Close();

			Assert.Equal(initialFileSize + 4096 * 2, new FileInfo(filename).Length); //file size should increase by 4KB x 2
		}

		[Fact]
		public void when_expanding_an_aligned_file_by_one_byte() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(0, SeekOrigin.End);

			Assert.Equal(initialFileSize, stream.Position); //verify position
			stream.SetLength(initialFileSize + 1); //expand file by 1 byte
			Assert.Equal(initialFileSize, stream.Position); //position should not change
			
			stream.Close();

			Assert.Equal(initialFileSize + 4096, new FileInfo(filename).Length); //file size should increase by 4KB
		}

		[Fact]
		public void when_truncating_an_aligned_file_by_one_page() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(0, SeekOrigin.End);

			Assert.Equal(initialFileSize, stream.Position); //verify position
			stream.SetLength(initialFileSize - 4096); //truncate file by 4KB
			Assert.Equal(initialFileSize - 4096, stream.Position); //position should decrease by 4KB
			
			stream.Close();

			Assert.Equal(initialFileSize - 4096, new FileInfo(filename).Length); //file size should decrease by 4KB
		}

		[Fact]
		public void when_truncating_an_aligned_file_by_one_page_and_position_one_page_from_eof() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(-4096, SeekOrigin.End);

			Assert.Equal(initialFileSize - 4096, stream.Position); //verify position
			stream.SetLength(initialFileSize - 4096); //truncate file by 4KB
			Assert.Equal(initialFileSize - 4096, stream.Position); //position should not change
			
			stream.Close();

			Assert.Equal(initialFileSize - 4096, new FileInfo(filename).Length); //file size should decrease by 4KB
		}

		[Fact]
		public void when_truncating_an_aligned_file_by_one_byte_less_than_a_page() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(0, SeekOrigin.End);

			Assert.Equal(initialFileSize, stream.Position); //verify position
			stream.SetLength(initialFileSize - 4095); //truncate file by 4KB - 1
			Assert.Equal(initialFileSize, stream.Position); //position should not change
			
			stream.Close();

			Assert.Equal(initialFileSize, new FileInfo(filename).Length); //file size should not change
		}

		[Fact]
		public void when_truncating_an_aligned_file_by_one_byte_more_than_a_page() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(0, SeekOrigin.End);

			Assert.Equal(initialFileSize, stream.Position); //verify position
			stream.SetLength(initialFileSize - 4097); //truncate file by 4KB + 1
			Assert.Equal(initialFileSize - 4096, stream.Position); //position should decrease by 4KB 
			
			stream.Close();

			Assert.Equal(initialFileSize - 4096, new FileInfo(filename).Length); //file size should decrease by 4KB
		}

		[Fact]
		public void when_truncating_an_aligned_file_by_one_byte() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096);
			
			var initialFileSize = 4096 * 1024;
			stream.SetLength(initialFileSize); //initial size of 4MB

			stream.Seek(0, SeekOrigin.End);

			Assert.Equal(initialFileSize, stream.Position); //verify position
			stream.SetLength(initialFileSize - 1); //truncate file by 1 byte
			Assert.Equal(initialFileSize, stream.Position); //position should not change
			
			stream.Close();

			Assert.Equal(initialFileSize, new FileInfo(filename).Length); //file size should not change
		}

		[Fact]
		public void when_writing_less_than_buffer() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = GetBytes(255);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Write(bytes, 0, bytes.Length);
				Assert.Equal(bytes.Length, stream.Position);
				Assert.Equal(0, new FileInfo(filename).Length);
			}
		}

		[Fact]
		public void when_writing_more_than_buffer() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = GetBytes(9000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Write(bytes, 0, bytes.Length);
				Assert.Equal(4096 * 2, new FileInfo(filename).Length);
				var read = ReadAllBytesShared(filename);
				for (var i = 0; i < 4096 * 2; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}

		[Fact]
		public void when_writing_less_than_buffer_and_closing() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = GetBytes(255);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Write(bytes, 0, bytes.Length);
			}

			Assert.Equal(4096, new FileInfo(filename).Length);
			var read = ReadAllBytesShared(filename);

			for (var i = 0; i < 255; i++) {
				Assert.Equal(i % 256, read[i]);
			}
		}

		[Fact(Skip = "Requires a 4gb file")]
		public void when_seeking_greater_than_2gb() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var GIGABYTE = 1024L * 1024L * 1024L;
			try {
				using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
					FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
					stream.SetLength(4L * GIGABYTE);
					stream.Seek(3L * GIGABYTE, SeekOrigin.Begin);
				}
			} finally {
				File.Delete(filename);
			}
		}

		[Fact]
		public void when_writing_less_than_buffer_and_seeking() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = GetBytes(255);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Write(bytes, 0, bytes.Length);
				stream.Seek(0, SeekOrigin.Begin);
				Assert.Equal(0, stream.Position);
				Assert.Equal(4096, new FileInfo(filename).Length);
				var read = ReadAllBytesShared(filename);

				for (var i = 0; i < 255; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}

		[Fact]
		public void when_writing_exact_to_alignment_and_writing_again() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = GetBytes(4096);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Write(bytes, 0, bytes.Length);
				Assert.Equal(4096, stream.Position);
				bytes = GetBytes(15);
				stream.Write(bytes, 0, bytes.Length);
				Assert.Equal(4111, stream.Position);
				stream.Flush();
				Assert.Equal(4111, stream.Position);
				Assert.Equal(8192, new FileInfo(filename).Length);
				var read = ReadAllBytesShared(filename);

				for (var i = 0; i < 255; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}

		[Fact]
		public void when_writing_then_seeking_exact_to_alignment_and_writing_again() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = GetBytes(8192);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Write(bytes, 0, 5012);
				Assert.Equal(5012, stream.Position);
				stream.Seek(4096, SeekOrigin.Begin);
				Assert.Equal(4096, stream.Position);
				bytes = GetBytes(15);
				stream.Write(bytes, 0, bytes.Length);
				Assert.Equal(4111, stream.Position);
				stream.Flush();
				Assert.Equal(4111, stream.Position);
				Assert.Equal(8192, new FileInfo(filename).Length);
				var read = ReadAllBytesShared(filename);

				for (var i = 0; i < 255; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}


		[Fact]
		public void when_seeking_non_exact_to_zero_block_and_writing() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 4096 * 64);
			var bytes = GetBytes(512);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Seek(128, SeekOrigin.Begin);
				stream.Write(bytes, 0, bytes.Length);
				stream.Flush();
			}

			using (var stream = new FileStream(filename, FileMode.Open)) {
				var read = new byte[128];
				stream.Read(read, 0, 128);
				for (var i = 0; i < read.Length; i++) {
					Assert.Equal(i, read[i]);
				}
			}
		}

		[Fact]
		public void when_writing_multiple_times() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = GetBytes(256);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Write(bytes, 0, bytes.Length);
				Assert.Equal(256, stream.Position);
				stream.Flush();
				Assert.Equal(256, stream.Position);
				stream.Write(bytes, 0, bytes.Length);
				Assert.Equal(512, stream.Position);
				stream.Flush();
				Assert.Equal(512, stream.Position);
				Assert.Equal(4096, new FileInfo(filename).Length);
				var read = ReadAllBytesShared(filename);

				for (var i = 0; i < 512; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}


		[Fact]
		public void when_reading_multiple_times() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 20000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Seek(4096 + 15, SeekOrigin.Begin);
				var read = new byte[1000];
				stream.Read(read, 0, 500);
				Assert.Equal(4096 + 15 + 500, stream.Position);
				stream.Read(read, 500, 500);
				Assert.Equal(4096 + 15 + 1000, stream.Position);
				for (var i = 0; i < read.Length; i++) {
					Assert.Equal((i + 15) % 256, read[i]);
				}
			}
		}

		[Fact]
		public void when_reading_multiple_times_no_seek() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 20000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				var read = new byte[1000];
				stream.Read(read, 0, 500);
				Assert.Equal(500, stream.Position);
				stream.Read(read, 500, 500);
				Assert.Equal(1000, stream.Position);
				for (var i = 0; i < read.Length; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}

		[Fact]
		public void when_reading_multiple_times_over_page_size() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 20000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				var read = new byte[6000];
				stream.Read(read, 0, 3000);
				Assert.Equal(3000, stream.Position);
				var total = stream.Read(read, 3000, 3000);
				Assert.Equal(3000, total);
				Assert.Equal(6000, stream.Position);
				for (var i = 0; i < read.Length; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}

		[Fact]
		public void when_reading_multiple_times_on_page_size() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 20000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				var read = new byte[6000];
				stream.Read(read, 0, 3000);
				Assert.Equal(3000, stream.Position);
				var total = stream.Read(read, 3000, 1096);
				Assert.Equal(1096, total);
				total = stream.Read(read, 4096, read.Length - 4096);
				Assert.Equal(read.Length - 4096, total);
				Assert.Equal(6000, stream.Position);
				for (var i = 0; i < read.Length; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}


		[Fact]
		public void when_reading_multiple_times_exact_page_size() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 4096 * 100 + 50);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				var read = new byte[4096];
				for (var i = 0; i < 100; i++) {
					var total = stream.Read(read, 0, 4096);
					Assert.Equal(4096 * (i + 1), stream.Position);
					Assert.Equal(4096, total);
					for (var j = 0; j < read.Length; j++) {
						Assert.Equal(j % 256, read[j]);
					}
				}

				var total2 = stream.Read(read, 0, 50);
				Assert.Equal(409600 + 50, stream.Position);
				Assert.Equal(50, total2);
				for (var j = 0; j < 50; j++) {
					Assert.Equal(j % 256, read[j]);
				}
			}
		}

		[Fact]
		public void when_reading_multiple_times_offset_page_size() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 4096 * 100 + 50);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Seek(50, SeekOrigin.Begin);
				var read = new byte[4096];
				for (var i = 0; i < 100; i++) {
					if (i == 99) Console.Write("");
					var total = stream.Read(read, 0, 4096);
					Assert.Equal(4096 * (i + 1) + 50, stream.Position);
					Assert.Equal(4096, total);
					for (var j = 0; j < read.Length; j++) {
						Assert.Equal((j + 50) % 256, read[j]);
					}
				}

				Assert.Equal(4096 * 100 + 50, stream.Position);
			}
		}

		[Fact]
		public void when_writing_more_than_buffer_and_closing() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = GetBytes(9000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Write(bytes, 0, bytes.Length);
				stream.Close();
				Assert.Equal(4096 * 3, new FileInfo(filename).Length);
				var read = File.ReadAllBytes(filename);
				for (var i = 0; i < 9000; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}

		[Fact]
		public void when_reading_on_aligned_buffer() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 20000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				var read = new byte[4096];
				stream.Read(read, 0, 4096);
				for (var i = 0; i < 4096; i++) {
					Assert.Equal(i % 256, read[i]);
				}
			}
		}

		[Fact]
		public void when_reading_on_unaligned_buffer() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 20000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Seek(15, SeekOrigin.Begin);
				var read = new byte[999];
				stream.Read(read, 0, read.Length);
				for (var i = 0; i < read.Length; i++) {
					Assert.Equal((i + 15) % 256, read[i]);
				}
			}
		}

		[Fact]
		public void seek_and_read_on_unaligned_buffer() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			MakeFile(filename, 20000);
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Seek(4096 + 15, SeekOrigin.Begin);
				Assert.Equal(4096 + 15, stream.Position);
				var read = new byte[999];
				stream.Read(read, 0, read.Length);
				Assert.Equal(4096 + 15 + 999, stream.Position);
				for (var i = 0; i < read.Length; i++) {
					Assert.Equal((i + 15) % 256, read[i]);
				}
			}
		}

		[Fact]
		public void seek_end_of_file() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Seek(0, SeekOrigin.End);
				Assert.Equal(stream.Length, stream.Position);
			}
		}

		[Fact]
		public void seek_origin_end_to_mid_of_file() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				stream.Seek(-30, SeekOrigin.End);
				Assert.Equal(stream.Length - 30, stream.Position);
			}
		}


		[Fact]
		public void seek_current_unimplemented() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());

			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				Assert.Throws<NotImplementedException>(() => stream.Seek(0, SeekOrigin.Current));
			}
		}


		[Fact]
		public void seek_write_seek_read_in_buffer() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			using (var stream = UnbufferedFileStream.Create(filename, FileMode.CreateNew, FileAccess.ReadWrite,
				FileShare.ReadWrite, false, 4096, 4096, false, 4096)) {
				var buffer = GetBytes(255);
				stream.Seek(4096 + 15, SeekOrigin.Begin);
				stream.Write(buffer, 0, buffer.Length);
				stream.Seek(4096 + 15, SeekOrigin.Begin);
				var read = new byte[255];
				stream.Read(read, 0, read.Length);
				for (var i = 0; i < read.Length; i++) {
					Assert.Equal(i % 255, read[i]);
				}
			}
		}

		[Fact]
		public void same_as_file_stream_on_reads() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = BuildBytes(4096 * 128);
			File.WriteAllBytes(filename, bytes);
			using (var f = new FileStream(filename, FileMode.Open, FileAccess.Read)) {
				using (
					var b = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.Read, FileShare.Read, false,
						4096, 4096, false, 4096)) {
					var readf = new byte[4096];
					var readb = new byte[4096];
					for (var i = 0; i < 128; i++) {
						var totalf = f.Read(readf, 0, 4096);
						var totalb = b.Read(readb, 0, 4096);
						Assert.Equal(totalf, totalb);
						for (var j = 0; j < 4096; j++) {
							Assert.Equal(readf[j], readb[j]);
						}
					}
				}
			}
		}


		[Fact]
		public void same_as_file_stream_on_reads_with_bigger_buffer() {
			var filename = GetFilePathFor(Guid.NewGuid().ToString());
			var bytes = BuildBytes(4096 * 128);
			File.WriteAllBytes(filename, bytes);
			using (var f = new FileStream(filename, FileMode.Open, FileAccess.Read)) {
				using (
					var b = UnbufferedFileStream.Create(filename, FileMode.Open, FileAccess.Read, FileShare.Read, false,
						4096, 4096 * 4, false, 4096)) {
					var readf = new byte[4096];
					var readb = new byte[4096];
					for (var i = 0; i < 128; i++) {
						var totalf = f.Read(readf, 0, 4096);
						var totalb = b.Read(readb, 0, 4096);
						Assert.Equal(totalf, totalb);
						for (var j = 0; j < 4096; j++) {
							Assert.Equal(readf[j], readb[j]);
						}
					}
				}
			}
		}

		private byte[] BuildBytes(int count) {
			var ret = new MemoryStream();
			for (int i = 0; i < count / 16; i++) {
				ret.Write(Guid.NewGuid().ToByteArray(), 0, 16);
			}

			return ret.ToArray();
		}


		private byte[] ReadAllBytesShared(string filename) {
			using (var fs = File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) {
				var ret = new byte[fs.Length];
				fs.Read(ret, 0, (int)fs.Length);
				return ret;
			}
		}

		private void MakeFile(string filename, int size) {
			var bytes = GetBytes(size);
			File.WriteAllBytes(filename, bytes);
		}

		private byte[] GetBytes(int size) {
			var ret = new byte[size];
			for (var i = 0; i < size; i++) {
				ret[i] = (byte)(i % 256);
			}

			return ret;
		}
	}
}
