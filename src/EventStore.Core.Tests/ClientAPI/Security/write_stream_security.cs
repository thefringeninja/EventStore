using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class write_stream_security : AuthenticationTestBase {
		[Fact]
		public async Task writing_to_all_is_never_allowed() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$all", null, null));
			await  Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$all", "user1", "pa$$1"));
			await  Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$all", "adm", "admpa$$"));
		}

		[Fact]
		public Task<NotAuthenticatedException> writing_with_not_existing_credentials_is_not_authenticated() {
			return Assert.ThrowsAsync<NotAuthenticatedException>(() => WriteStream("write-stream", "badlogin", "badpass"));
		}

		[Fact]
		public Task<AccessDeniedException> writing_to_stream_with_no_credentials_is_denied() {
			return Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("write-stream", null, null));
		}

		[Fact]
		public Task<AccessDeniedException> writing_to_stream_with_not_authorized_user_credentials_is_denied() {
			return Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("write-stream", "user2", "pa$$2"));
		}

		[Fact]
		public async Task writing_to_stream_with_authorized_user_credentials_succeeds() {
			await WriteStream("write-stream", "user1", "pa$$1");
		}

		[Fact]
		public async Task writing_to_stream_with_admin_user_credentials_succeeds() {
			await WriteStream("write-stream", "adm", "admpa$$");
		}


		[Fact]
		public async Task writing_to_no_acl_stream_succeeds_when_no_credentials_are_passed() {
			await WriteStream("noacl-stream", null, null);
		}

		[Fact]
		public Task<NotAuthenticatedException> writing_to_no_acl_stream_is_not_authenticated_when_not_existing_credentials_are_passed() {
			return Assert.ThrowsAsync<NotAuthenticatedException>(() => WriteStream("noacl-stream", "badlogin", "badpass"));
		}

		[Fact]
		public async Task writing_to_no_acl_stream_succeeds_when_any_existing_user_credentials_are_passed() {
			await WriteStream("noacl-stream", "user1", "pa$$1");
			await WriteStream("noacl-stream", "user2", "pa$$2");
		}

		[Fact]
		public async Task writing_to_no_acl_stream_succeeds_when_any_admin_user_credentials_are_passed() {
			await WriteStream("noacl-stream", "adm", "admpa$$");
		}


		[Fact]
		public async Task writing_to_all_access_normal_stream_succeeds_when_no_credentials_are_passed() {
			await WriteStream("normal-all", null, null);
		}

		[Fact]
		public Task<NotAuthenticatedException> writing_to_all_access_normal_stream_is_not_authenticated_when_not_existing_credentials_are_passed() {
			return Assert.ThrowsAsync<NotAuthenticatedException>(() => WriteStream("normal-all", "badlogin", "badpass"));
		}

		[Fact]
		public async Task writing_to_all_access_normal_stream_succeeds_when_any_existing_user_credentials_are_passed() {
			await WriteStream("normal-all", "user1", "pa$$1");
			await WriteStream("normal-all", "user2", "pa$$2");
		}

		[Fact]
		public async Task writing_to_all_access_normal_stream_succeeds_when_any_admin_user_credentials_are_passed() {
			await WriteStream("normal-all", "adm", "admpa$$");
		}
	}
}
