using System;
using System.IO;
using Xunit;

namespace EventStore.Core.Tests {
	public class mono_uritemplate_bug {
		[Fact]
		public void when_validating_a_uri_template_with_url_encoded_chars() {
			var template = new UriTemplate("/streams/$all?embed={embed}");
			var uri = new Uri("http://127.0.0.1/streams/$all");
			var baseaddress = new Uri("http://127.0.0.1");
			Assert.True(template.Match(baseaddress, uri) != null);
		}
	}


	public class mono_filestream_bug {
		[Fact(Skip="Known bug in Mono, waiting for fix.")]
		public void show_time() {
			const int pos = 1;
			const int bufferSize = 128;

			var filename = Path.GetTempFileName();
			File.WriteAllBytes(filename, new byte[pos + 1]); // init file with zeros

			var bytes = new byte[bufferSize + 1 /* THIS IS WHAT MAKES A BIG DIFFERENCE */];
			new Random().NextBytes(bytes);

			using (var file = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.Read,
				bufferSize, FileOptions.SequentialScan)) {
				file.Read(new byte[pos], 0, pos); // THIS READ IS CRITICAL, WITHOUT IT EVERYTHING WORKS
				Assert.Equal(pos,
					file.Position); // !!! here it says position is correct, but writes at different position !!!
				// file.Position = pos; // !!! this fixes test !!!
				file.Write(bytes, 0, bytes.Length);

				//Assert.Equal(pos + bytes.Length, file.Length); -- fails
			}

			using (var filestream = File.Open(filename, FileMode.Open, FileAccess.Read)) {
				var bb = new byte[bytes.Length];
				filestream.Position = pos;
				filestream.Read(bb, 0, bb.Length);
				Assert.Equal(bytes, bb);
			}
		}
	}
}
