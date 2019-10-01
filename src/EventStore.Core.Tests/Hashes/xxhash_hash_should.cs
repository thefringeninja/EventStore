using EventStore.Core.Index.Hashes;
using Xunit;

namespace EventStore.Core.Tests.Hashes {
	public class xxhash_hash_should {
		// calculated from reference XXhash implementation at http://code.google.com/p/xxhash/
		public static uint XXHashReferenceVerificationValue = 0x56D249B1;

		[Fact]
		public void pass_smhasher_verification_test() {
			Assert.True(SMHasher.VerificationTest(new XXHashUnsafe(), XXHashReferenceVerificationValue));
		}

		[Explicit, Trait("Category", "LongRunning")]
		public void pass_smhasher_sanity_test() {
			Assert.True(SMHasher.SanityTest(new XXHashUnsafe()));
		}
	}
}
