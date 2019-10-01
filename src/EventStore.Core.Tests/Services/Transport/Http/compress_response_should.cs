using System;
using System.Collections.Specialized;
using System.IO;
using System.IO.Compression;
using EventStore.Transport.Http.EntityManagement;
using Xunit;
using System.Net;
using System.Text;
using EventStore.Transport.Http;

namespace EventStore.Core.Tests.Services.Transport.Http {
	public class compress_response_should {
		private string inputData = "my test string 123456.";

		[Fact]
		public void with_gzip_compression_algo_data_is_gzipped() {
			var response =
				HttpEntityManager.CompressResponse(Encoding.ASCII.GetBytes(inputData), CompressionAlgorithms.Gzip);

			String uncompressed;

			using (var inputStream = new MemoryStream(response))
			using (var uncompressedStream = new GZipStream(inputStream, CompressionMode.Decompress))
			using (var outputStream = new MemoryStream()) {
				uncompressedStream.CopyTo(outputStream);
				uncompressed = Encoding.UTF8.GetString(outputStream.ToArray());
			}

			Assert.Equal(uncompressed, inputData);
		}

		[Fact]
		public void with_gzip_compression_algo_and_string_larger_than_50kb_data_is_gzipped() {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < 60 * 1024; i++) sb.Append("A");
			String testString = sb.ToString();

			var response =
				HttpEntityManager.CompressResponse(Encoding.ASCII.GetBytes(testString), CompressionAlgorithms.Gzip);

			String uncompressed;

			using (var inputStream = new MemoryStream(response))
			using (var uncompressedStream = new GZipStream(inputStream, CompressionMode.Decompress))
			using (var outputStream = new MemoryStream()) {
				uncompressedStream.CopyTo(outputStream);
				uncompressed = Encoding.UTF8.GetString(outputStream.ToArray());
			}

			Assert.Equal(uncompressed, testString);
		}

		[Fact]
		public void with_deflate_compression_algo_data_is_deflated() {
			var response =
				HttpEntityManager.CompressResponse(Encoding.ASCII.GetBytes(inputData), CompressionAlgorithms.Deflate);

			String uncompressed;

			using (var inputStream = new MemoryStream(response))
			using (var uncompressedStream = new DeflateStream(inputStream, CompressionMode.Decompress))
			using (var outputStream = new MemoryStream()) {
				uncompressedStream.CopyTo(outputStream);
				uncompressed = Encoding.UTF8.GetString(outputStream.ToArray());
			}

			Assert.Equal(uncompressed, inputData);
		}

		[Fact]
		public void with_deflate_compression_algo_and_string_larger_than_50kb_data_is_deflated() {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < 60 * 1024; i++) sb.Append("A");
			String testString = sb.ToString();

			var response =
				HttpEntityManager.CompressResponse(Encoding.ASCII.GetBytes(testString), CompressionAlgorithms.Deflate);

			String uncompressed;

			using (var inputStream = new MemoryStream(response))
			using (var uncompressedStream = new DeflateStream(inputStream, CompressionMode.Decompress))
			using (var outputStream = new MemoryStream()) {
				uncompressedStream.CopyTo(outputStream);
				uncompressed = Encoding.UTF8.GetString(outputStream.ToArray());
			}

			Assert.Equal(uncompressed, testString);
		}

		[Fact]
		public void with_invalid_compression_algo_data_remains_the_same() {
			var response =
				HttpEntityManager.CompressResponse(Encoding.ASCII.GetBytes(inputData), "invalid_compression_algo");
			Assert.Equal(Encoding.ASCII.GetString(response), inputData);
		}

		[Fact]
		public void with_null_compression_algo_data_remains_the_same() {
			var response = HttpEntityManager.CompressResponse(Encoding.ASCII.GetBytes(inputData), null);
			Assert.Equal(Encoding.ASCII.GetString(response), inputData);
		}
	}
}
