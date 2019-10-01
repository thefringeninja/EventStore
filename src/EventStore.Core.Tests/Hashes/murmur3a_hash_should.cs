using EventStore.Core.Index.Hashes;
using Xunit;

namespace EventStore.Core.Tests.Hashes {
	public class murmur3a_hash_should {
		public static uint Murmur3AReferenceVerificationValue = 0xB0F57EE3; // taken from SMHasher

		[Fact]
		public void pass_smhasher_verification_test() {
			Assert.True(SMHasher.VerificationTest(new Murmur3AUnsafe(), Murmur3AReferenceVerificationValue));
		}

		[Explicit, Trait("Category", "LongRunning")]
		public void pass_smhasher_sanity_test() {
			Assert.True(SMHasher.SanityTest(new Murmur3AUnsafe()));
		}
	}
}
