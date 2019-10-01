using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Services;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class write_stream_meta_security : AuthenticationTestBase {
		[Fact]
		public Task writing_meta_with_not_existing_credentials_is_not_authenticated() {
			return Assert.ThrowsAsync<NotAuthenticatedException>(() => WriteMeta("metawrite-stream", "badlogin", "badpass", "user1"));
		}

		[Fact]
		public Task writing_meta_to_stream_with_no_credentials_is_denied() {
			return Assert.ThrowsAsync<AccessDeniedException>(() => WriteMeta("metawrite-stream", null, null, "user1"));
		}

		[Fact]
		public Task writing_meta_to_stream_with_not_authorized_user_credentials_is_denied() {
			return Assert.ThrowsAsync<AccessDeniedException>(() => WriteMeta("metawrite-stream", "user2", "pa$$2", "user1"));
		}

		[Fact]
		public async Task writing_meta_to_stream_with_authorized_user_credentials_succeeds() {
			await WriteMeta("metawrite-stream", "user1", "pa$$1", "user1");
		}

		[Fact]
		public async Task writing_meta_to_stream_with_admin_user_credentials_succeeds() {
			await WriteMeta("metawrite-stream", "adm", "admpa$$", "user1");
		}


		[Fact]
		public async Task writing_meta_to_no_acl_stream_succeeds_when_no_credentials_are_passed() {
			await WriteMeta("noacl-stream", null, null, null);
		}

		[Fact]
		public Task writing_meta_to_no_acl_stream_is_not_authenticated_when_not_existing_credentials_are_passed() {
			return Assert.ThrowsAsync<NotAuthenticatedException>(() => WriteMeta("noacl-stream", "badlogin", "badpass", null));
		}

		[Fact]
		public async Task writing_meta_to_no_acl_stream_succeeds_when_any_existing_user_credentials_are_passed() {
			await WriteMeta("noacl-stream", "user1", "pa$$1", null);
			await WriteMeta("noacl-stream", "user2", "pa$$2", null);
		}

		[Fact]
		public async Task writing_meta_to_no_acl_stream_succeeds_when_admin_user_credentials_are_passed() {
			await WriteMeta("noacl-stream", "adm", "admpa$$", null);
		}


		[Fact]
		public async Task writing_meta_to_all_access_normal_stream_succeeds_when_no_credentials_are_passed() {
			await WriteMeta("normal-all", null, null, SystemRoles.All);
		}

		[Fact]
		public Task writing_meta_to_all_access_normal_stream_is_not_authenticated_when_not_existing_credentials_are_passed() {
			return Assert.ThrowsAsync<NotAuthenticatedException>(() => WriteMeta("normal-all", "badlogin", "badpass", SystemRoles.All));
		}

		[Fact]
		public async Task writing_meta_to_all_access_normal_stream_succeeds_when_any_existing_user_credentials_are_passed() {
			await WriteMeta("normal-all", "user1", "pa$$1", SystemRoles.All);
			await WriteMeta("normal-all", "user2", "pa$$2", SystemRoles.All);
		}

		[Fact]
		public async Task writing_meta_to_all_access_normal_stream_succeeds_when_admin_user_credentials_are_passed() {
			await WriteMeta("normal-all", "adm", "admpa$$", SystemRoles.All);
		}
	}
}
