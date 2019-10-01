using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace EventStore.Core.Tests {
	public abstract class SpecificationWithFile : IAsyncLifetime {
		protected string Filename;

		public virtual void SetUp() {
			var typeName = GetType().Name.Length > 30 ? GetType().Name.Substring(0, 30) : GetType().Name;
			Filename = Path.Combine(Path.GetTempPath(), string.Format("{0}-{1}", Guid.NewGuid(), typeName));
		}

		public virtual void TearDown() {
			if (File.Exists(Filename))
				File.Delete(Filename);
		}

		public Task InitializeAsync() {
			SetUp();
			return Task.CompletedTask;
		}

		public Task DisposeAsync() {
			TearDown();
			return Task.CompletedTask;
		}
	}
}
