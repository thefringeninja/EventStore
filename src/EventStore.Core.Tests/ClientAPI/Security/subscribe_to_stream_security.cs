using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class subscribe_to_stream_security : AuthenticationTestBase {
		[Fact]
		public async Task subscribing_to_stream_with_not_existing_credentials_is_not_authenticated() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => SubscribeToStream("read-stream", "badlogin", "badpass"));
		}

		[Fact]
		public async Task subscribing_to_stream_with_no_credentials_is_denied() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToStream("read-stream", null, null));
		}

		[Fact]
		public async Task subscribing_to_stream_with_not_authorized_user_credentials_is_denied() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToStream("read-stream", "user2", "pa$$2"));
		}

		[Fact]
		public async Task reading_stream_with_authorized_user_credentials_succeeds() {
			await SubscribeToStream("read-stream", "user1", "pa$$1");
		}

		[Fact]
		public async Task reading_stream_with_admin_user_credentials_succeeds() {
			await SubscribeToStream("read-stream", "adm", "admpa$$");
		}


		[Fact]
		public async Task subscribing_to_no_acl_stream_succeeds_when_no_credentials_are_passed() {
			await SubscribeToStream("noacl-stream", null, null);
		}

		[Fact]
		public async Task subscribing_to_no_acl_stream_is_not_authenticated_when_not_existing_credentials_are_passed() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => SubscribeToStream("noacl-stream", "badlogin", "badpass"));
		}

		[Fact]
		public async Task subscribing_to_no_acl_stream_succeeds_when_any_existing_user_credentials_are_passed() {
			await SubscribeToStream("noacl-stream", "user1", "pa$$1");
			await SubscribeToStream("noacl-stream", "user2", "pa$$2");
		}

		[Fact]
		public async Task subscribing_to_no_acl_stream_succeeds_when_admin_user_credentials_are_passed() {
			await SubscribeToStream("noacl-stream", "adm", "admpa$$");
		}


		[Fact]
		public async Task subscribing_to_all_access_normal_stream_succeeds_when_no_credentials_are_passed() {
			await SubscribeToStream("normal-all", null, null);
		}

		[Fact]
		public async Task
			subscribing_to_all_access_normal_stream_is_not_authenticated_when_not_existing_credentials_are_passed() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => SubscribeToStream("normal-all", "badlogin", "badpass"));
		}

		[Fact]
		public async Task subscribing_to_all_access_normal_stream_succeeds_when_any_existing_user_credentials_are_passed() {
			await SubscribeToStream("normal-all", "user1", "pa$$1");
			await SubscribeToStream("normal-all", "user2", "pa$$2");
		}

		[Fact]
		public async Task subscribing_to_all_access_normal_streamm_succeeds_when_admin_user_credentials_are_passed() {
			await SubscribeToStream("normal-all", "adm", "admpa$$");
		}
	}
}
