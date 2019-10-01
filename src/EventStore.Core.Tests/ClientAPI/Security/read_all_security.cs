using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class read_all_security : AuthenticationTestBase {
		[Fact]
		public async Task reading_all_with_not_existing_credentials_is_not_authenticated() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadAllForward("badlogin", "badpass"));
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadAllBackward("badlogin", "badpass"));
		}

		[Fact]
		public async Task reading_all_with_no_credentials_is_denied() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadAllForward(null, null));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadAllBackward(null, null));
		}

		[Fact]
		public async Task reading_all_with_not_authorized_user_credentials_is_denied() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadAllForward("user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadAllBackward("user2", "pa$$2"));
		}

		[Fact]
		public async Task reading_all_with_authorized_user_credentials_succeeds() {
			await ReadAllForward("user1", "pa$$1");
			await ReadAllBackward("user1", "pa$$1");
		}

		[Fact]
		public async Task reading_all_with_admin_credentials_succeeds() {
			await ReadAllForward("adm", "admpa$$");
			await ReadAllBackward("adm", "admpa$$");
		}
	}
}
