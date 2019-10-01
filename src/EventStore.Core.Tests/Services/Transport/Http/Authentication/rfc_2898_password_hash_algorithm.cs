using EventStore.Core.Services.Transport.Http.Authentication;
using Xunit;

namespace EventStore.Core.Tests.Services.Transport.Http.Authentication {
	namespace rfc_2898_password_hash_algorithm {
		public class when_hashing_a_password {
			private Rfc2898PasswordHashAlgorithm _algorithm;
			private string _password;
			private string _hash;
			private string _salt;

			public when_hashing_a_password() {
				_password = "Pa55w0rd!";
				_algorithm = new Rfc2898PasswordHashAlgorithm();
				_algorithm.Hash(_password, out _hash, out _salt);
			}

			[Fact]
			public void verifies_correct_password() {
				Assert.True(_algorithm.Verify(_password, _hash, _salt));
			}

			[Fact]
			public void does_not_verify_incorrect_password() {
				Assert.True(!_algorithm.Verify(_password.ToUpper(), _hash, _salt));
			}
		}

		public class when_hashing_a_password_twice {
			private Rfc2898PasswordHashAlgorithm _algorithm;
			private string _password;
			private string _hash1;
			private string _salt1;
			private string _hash2;
			private string _salt2;

			public when_hashing_a_password_twice() {
				_password = "Pa55w0rd!";
				_algorithm = new Rfc2898PasswordHashAlgorithm();
				_algorithm.Hash(_password, out _hash1, out _salt1);
				_algorithm.Hash(_password, out _hash2, out _salt2);
			}

			[Fact]
			public void uses_different_salt() {
				Assert.True(_salt1 != _salt2);
			}

			[Fact]
			public void generates_different_hashes() {
				Assert.True(_hash1 != _hash2);
			}
		}
	}
}
