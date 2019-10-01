using EventStore.Core.Index.Hashes;
using Xunit;

namespace EventStore.Core.Tests.Hashes {
	public class murmur2_hash_should {
		public static uint Murmur2ReferenceVerificationValue = 0x27864C1E; // taken from SMHasher

		[Fact]
		public void pass_smhasher_verification_test() {
			Assert.True(SMHasher.VerificationTest(new Murmur2Unsafe(), Murmur2ReferenceVerificationValue));
		}

		[Explicit, Trait("Category", "LongRunning")]
		public void pass_smhasher_sanity_test() {
			Assert.True(SMHasher.SanityTest(new Murmur2Unsafe()));
		}
	}
}
