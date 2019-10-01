using System;
using System.IO;
using System.Security.Cryptography;
using EventStore.Core.Util;
using Xunit;

namespace EventStore.Core.Tests.Hashes {
	public class MD5HashTests {
		[Fact]
		public void does_not_include_previous_data_in_stream() {
			var bytes = new byte[1024];
			for (int i = 15; i < 1024; i++) {
				bytes[i] = (byte)(i % 255);
			}

			var stream = new MemoryStream(bytes);
			stream.Seek(16, SeekOrigin.Begin);
			var hash = MD5Hash.GetHashFor(stream);
			Array.Copy(hash, 0, bytes, 0, hash.Length);
			stream.Seek(16, SeekOrigin.Begin);
			var hash2 = MD5Hash.GetHashFor(stream);
			Assert.Equal(16, hash.Length);
			Assert.Equal(hash, hash2);
		}

		[Fact]
		public void changing_data_in_stream_results_in_different_hash() {
			var bytes = new byte[1024];
			for (int i = 15; i < 1024; i++) {
				bytes[i] = (byte)(i % 255);
			}

			var stream = new MemoryStream(bytes);
			stream.Seek(16, SeekOrigin.Begin);
			var hash = MD5Hash.GetHashFor(stream);
			bytes[243] = 17;
			stream.Seek(16, SeekOrigin.Begin);
			var hash2 = MD5Hash.GetHashFor(stream);
			Assert.NotEqual(hash, hash2);
		}

		[Fact]
		public void includes_correct_substream_data() {
			var bytes = new byte[1024];
			for (int i = 15; i < 1024; i++) {
				bytes[i] = (byte)(i % 255);
			}

			var stream = new MemoryStream(bytes);
			var hash = MD5Hash.GetHashFor(stream, 16, bytes.Length - 32);

			using (var md5 = MD5.Create()) {
				var referenceHash = md5.ComputeHash(bytes, 16, bytes.Length - 32);
				Assert.Equal(16, hash.Length);
				Assert.Equal(referenceHash, hash);
			}
		}

		[Explicit, Trait("Category", "LongRunning")]
		public void randomized_hash_verification_test() {
			var buf = new byte[1024];
			var seed = Environment.TickCount;
			Console.WriteLine("Seed: {0}", seed);
			new Random().NextBytes(buf);

			var stream = new MemoryStream(buf);

			using (var md5 = MD5.Create()) {
				for (int i = 0; i < buf.Length; ++i) {
					for (int j = i; j < buf.Length + 10; ++j) {
						var referenceHash = md5.ComputeHash(buf, i, Math.Min(buf.Length - i, j - i + 1));
						var hash = MD5Hash.GetHashFor(stream, i, j - i + 1);
						Assert.Equal(16, hash.Length);
						Assert.Equal(referenceHash, hash);
					}
				}
			}
		}
	}
}
