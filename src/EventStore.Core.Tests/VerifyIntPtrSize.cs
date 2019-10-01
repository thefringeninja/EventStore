using System;
using Xunit;

namespace EventStore.Core.Tests {
	public class VerifyIntPtrSize {
		[Fact]
		public void TestIntPtrSize() {
			Assert.Equal(8, IntPtr.Size);
		}
	}
}
