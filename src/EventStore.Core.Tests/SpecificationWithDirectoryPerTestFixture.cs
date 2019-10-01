using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace EventStore.Core.Tests {
	public class SpecificationWithDirectoryPerTestFixture : IAsyncLifetime {
		public readonly string PathName;

		public SpecificationWithDirectoryPerTestFixture() {
			var typeName = GetType().Name.Length > 30 ? GetType().Name.Substring(0, 30) : GetType().Name;
			PathName = Path.Combine(Path.GetTempPath(), string.Format("{0}-{1}", Guid.NewGuid(), typeName));
			Directory.CreateDirectory(PathName);
		}

		protected string GetTempFilePath() {
			var typeName = GetType().Name.Length > 30 ? GetType().Name.Substring(0, 30) : GetType().Name;
			return Path.Combine(PathName, string.Format("{0}-{1}", Guid.NewGuid(), typeName));
		}

		protected string GetFilePathFor(string fileName) {
			return Path.Combine(PathName, fileName);
		}

		public virtual Task TestFixtureSetUp() {
			return Task.CompletedTask;
		}

		public virtual Task TestFixtureTearDown() {
			//kill whole tree
			ForceDeleteDirectory(PathName);

			return Task.CompletedTask;
		}

		private static void ForceDeleteDirectory(string path) {
			var directory = new DirectoryInfo(path) {Attributes = FileAttributes.Normal};
			foreach (var info in directory.GetFileSystemInfos("*", SearchOption.AllDirectories)) {
				info.Attributes = FileAttributes.Normal;
			}

			directory.Delete(true);
		}

		public Task InitializeAsync() => TestFixtureSetUp();

		public Task DisposeAsync() => TestFixtureTearDown();
	}
}
