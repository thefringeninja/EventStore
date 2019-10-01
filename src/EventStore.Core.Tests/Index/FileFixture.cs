using System;
using System.IO;
using EventStore.Core.Index;

namespace EventStore.Core.Tests.Index {
	public abstract class FileFixture : IDisposable {
		public readonly string FileName;

		protected FileFixture() {
			var typeName = GetType().Name.Length > 30 ? GetType().Name.Substring(0, 30) : GetType().Name;
			FileName = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}-{typeName}");
		}

		public virtual void Dispose() {
			if (File.Exists(FileName))
				File.Delete(FileName);
		}
	}

	public abstract class PTableReadFixture : FileFixture {
		private readonly int _midpointCacheDepth;
		protected readonly byte _ptableVersion = PTableVersions.IndexV1;

		public readonly PTable PTable;
		private bool _skipIndexVerify;

		protected PTableReadFixture(byte ptableVersion, bool skipIndexVerify, int midpointCacheDepth) {
			_ptableVersion = ptableVersion;
			_skipIndexVerify = skipIndexVerify;
			_midpointCacheDepth = midpointCacheDepth;

			var table = new HashListMemTable(_ptableVersion, maxSize: 50);

			AddItemsForScenario(table);

			PTable = PTable.FromMemtable(table, FileName, cacheDepth: _midpointCacheDepth,
				skipIndexVerify: _skipIndexVerify);
		}

		protected abstract void AddItemsForScenario(IMemTable memTable);

		public override void Dispose() {
			PTable?.Dispose();
			base.Dispose();
		}
	}
}
