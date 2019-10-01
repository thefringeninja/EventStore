using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Index;
using EventStore.Core.Util;
using Xunit;

namespace EventStore.Core.Tests.Index.IndexVAny {
	public class saving_empty_index_to_a_file : SpecificationWithDirectoryPerTestFixture {
		private string _filename;
		private IndexMap _map;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			_filename = GetFilePathFor("indexfile");
			_map = IndexMapTestFactory.FromFile(_filename);
			_map.SaveToFile(_filename);
		}

		[Fact]
		public void the_file_exists() {
			Assert.True(File.Exists(_filename));
		}

		[Fact]
		public void the_file_contains_correct_data() {
			using (var fs = File.OpenRead(_filename))
			using (var reader = new StreamReader(fs)) {
				var text = reader.ReadToEnd();
				var lines = text.Replace("\r", "").Split('\n');

				fs.Position = 32;
				var md5 = MD5Hash.GetHashFor(fs);
				var md5String = BitConverter.ToString(md5).Replace("-", "");

				Assert.Equal(5, lines.Count());
				Assert.Equal(md5String, lines[0]);
				Assert.Equal(IndexMap.IndexMapVersion.ToString(), lines[1]);
				Assert.Equal("-1/-1", lines[2]);
				Assert.Equal($"{int.MaxValue}", lines[3]);
				Assert.Equal("", lines[4]);
			}
		}

		[Fact]
		public void saved_file_could_be_read_correctly_and_without_errors() {
			var map = IndexMapTestFactory.FromFile(_filename);

			Assert.Equal(-1, map.PrepareCheckpoint);
			Assert.Equal(-1, map.CommitCheckpoint);
		}
	}
}
