using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class subscribe_to_all_security : AuthenticationTestBase {

		[Fact]
		public async Task subscribing_to_all_with_not_existing_credentials_is_not_authenticated() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => SubscribeToAll("badlogin", "badpass"));
		}

		[Fact]
		public async Task subscribing_to_all_with_no_credentials_is_denied() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToAll(null, null));
		}

		[Fact]
		public async Task subscribing_to_all_with_not_authorized_user_credentials_is_denied() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToAll("user2", "pa$$2"));
		}

		[Fact]
		public async Task subscribing_to_all_with_authorized_user_credentials_succeeds() {
			await SubscribeToAll("user1", "pa$$1");
		}

		[Fact]
		public async Task subscribing_to_all_with_admin_user_credentials_succeeds() {
			await SubscribeToAll("adm", "admpa$$");
		}
	}
}
