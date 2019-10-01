using System;
using System.IO;
using Xunit;

namespace EventStore.Core.Tests {
	public class SpecificationWithFilePerTestFixture : IDisposable {
		protected string Filename;
		
		public SpecificationWithFilePerTestFixture() {
			var typeName = GetType().Name.Length > 30 ? GetType().Name.Substring(0, 30) : GetType().Name;
			Filename = Path.Combine(Path.GetTempPath(), string.Format("{0}-{1}", Guid.NewGuid(), typeName));
		}

		public virtual void Dispose() {
			if (File.Exists(Filename))
				File.Delete(Filename);
		}
	}
}
