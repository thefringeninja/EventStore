using System;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.DeletingStream {
	public class is_stream_deleted_should : ReadIndexTestScenario {
		protected override void WriteTestScenario() {
		}

		[Fact]
		public void crash_on_null_stream_argument() {
			Assert.Throws<ArgumentNullException>(() => ReadIndex.IsStreamDeleted(null));
		}

		[Fact]
		public void throw_on_empty_stream_argument() {
			Assert.Throws<ArgumentNullException>(() => ReadIndex.IsStreamDeleted(string.Empty));
		}
	}
}
