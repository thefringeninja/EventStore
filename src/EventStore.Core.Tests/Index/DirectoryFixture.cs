using System;
using System.IO;

namespace EventStore.Core.Tests.Index {
	public abstract class DirectoryFixture : IDisposable {
		public string PathName { get; }

		protected DirectoryFixture() {
			var typeName = GetType().Name.Length > 30 ? GetType().Name.Substring(0, 30) : GetType().Name;
			PathName = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}-{typeName}");
			Directory.CreateDirectory(PathName);
		}

		public string GetTempFilePath() {
			var typeName = GetType().Name;
			return Path.Combine(PathName,
				$"{Guid.NewGuid()}-{(typeName.Length > 30 ? typeName.Substring(0, 30) : typeName)}");
		}

		protected string GetFilePathFor(string fileName) => Path.Combine(PathName, fileName);

		private static void ForceDeleteDirectory(string path) {
			var directory = new DirectoryInfo(path) {Attributes = FileAttributes.Normal};
			foreach (var info in directory.GetFileSystemInfos("*", SearchOption.AllDirectories)) {
				info.Attributes = FileAttributes.Normal;
			}

			directory.Delete(true);
		}

		public virtual void Dispose() => ForceDeleteDirectory(PathName);
	}
}
