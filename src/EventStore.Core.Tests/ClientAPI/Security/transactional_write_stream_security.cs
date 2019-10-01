using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using Xunit;
using static Xunit.Assert;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class transactional_write_stream_security : AuthenticationTestBase {
		[Fact]
		public async Task starting_transaction_with_not_existing_credentials_is_not_authenticated() {
			await ThrowsAsync<NotAuthenticatedException>(() => TransStart("write-stream", "badlogin", "badpass"));
		}

		[Fact]
		public async Task starting_transaction_to_stream_with_no_credentials_is_denied() {
			await ThrowsAsync<AccessDeniedException>(() => TransStart("write-stream", null, null));
		}

		[Fact]
		public async Task starting_transaction_to_stream_with_not_authorized_user_credentials_is_denied() {
			await ThrowsAsync<AccessDeniedException>(() => TransStart("write-stream", "user2", "pa$$2"));
		}

		[Fact]
		public async Task starting_transaction_to_stream_with_authorized_user_credentials_succeeds() {
			await TransStart("write-stream", "user1", "pa$$1");
		}

		[Fact]
		public async Task starting_transaction_to_stream_with_admin_user_credentials_succeeds() {
			await TransStart("write-stream", "adm", "admpa$$");
		}


		[Fact]
		public async Task committing_transaction_with_not_existing_credentials_is_not_authenticated() {
			var transId = (await TransStart("write-stream", "user1", "pa$$1")).TransactionId;
			var t2 = Connection.ContinueTransaction(transId, new UserCredentials("badlogin", "badpass"));
            await t2.WriteAsync(CreateEvents());
			await ThrowsAsync<NotAuthenticatedException>(() => t2.CommitAsync());
		}

		[Fact]
		public async Task committing_transaction_to_stream_with_no_credentials_is_denied() {
			var transId = (await TransStart("write-stream", "user1", "pa$$1")).TransactionId;
			var t2 = Connection.ContinueTransaction(transId);
            await t2.WriteAsync();
			await ThrowsAsync<AccessDeniedException>(() => t2.CommitAsync());
		}

		[Fact]
		public async Task committing_transaction_to_stream_with_not_authorized_user_credentials_is_denied() {
			var transId = (await TransStart("write-stream", "user1", "pa$$1")).TransactionId;
			var t2 = Connection.ContinueTransaction(transId, new UserCredentials("user2", "pa$$2"));
            await t2.WriteAsync();
			await ThrowsAsync<AccessDeniedException>(() => t2.CommitAsync());
		}

		[Fact]
		public async Task committing_transaction_to_stream_with_authorized_user_credentials_succeeds() {
			var transId = (await TransStart("write-stream", "user1", "pa$$1")).TransactionId;
			var t2 = Connection.ContinueTransaction(transId, new UserCredentials("user1", "pa$$1"));
            await t2.WriteAsync();
			await t2.CommitAsync();
		}

		[Fact]
		public async Task committing_transaction_to_stream_with_admin_user_credentials_succeeds() {
			var transId = (await TransStart("write-stream", "user1", "pa$$1")).TransactionId;
			var t2 = Connection.ContinueTransaction(transId, new UserCredentials("adm", "admpa$$"));
            await t2.WriteAsync();
			await t2.CommitAsync();
		}


		[Fact]
		public void transaction_to_no_acl_stream_succeeds_when_no_credentials_are_passed() {
			ExpectNoException(async () => {
				var t =  await TransStart("noacl-stream", null, null);
                await t.WriteAsync(CreateEvents());
                await t.CommitAsync();
			});
		}

		[Fact]
		public async Task transaction_to_no_acl_stream_is_not_authenticated_when_not_existing_credentials_are_passed() {
			await ThrowsAsync<NotAuthenticatedException>(() => TransStart("noacl-stream", "badlogin", "badpass"));
		}

		[Fact]
		public void transaction_to_no_acl_stream_succeeds_when_any_existing_user_credentials_are_passed() {
			ExpectNoException(async () => {
				var t = await TransStart("noacl-stream", "user1", "pa$$1");
                await t.WriteAsync(CreateEvents());
                await t.CommitAsync();
			});
			ExpectNoException(async () => {
				var t = await TransStart("noacl-stream", "user2", "pa$$2");
                await t.WriteAsync(CreateEvents());
                await t.CommitAsync();
			});
		}

		[Fact]
		public void transaction_to_no_acl_stream_succeeds_when_admin_user_credentials_are_passed() {
			ExpectNoException(async () => {
				var t = await TransStart("noacl-stream", "adm", "admpa$$");
                await t.WriteAsync(CreateEvents());
                await t.CommitAsync();
			});
		}


		[Fact]
		public void transaction_to_all_access_normal_stream_succeeds_when_no_credentials_are_passed() {
			ExpectNoException(async () => {
				var t = await TransStart("normal-all", null, null);
                await t.WriteAsync(CreateEvents());
                await t.CommitAsync();
			});
		}

		[Fact]
		public async Task
			transaction_to_all_access_normal_stream_is_not_authenticated_when_not_existing_credentials_are_passed() {
			await ThrowsAsync<NotAuthenticatedException>(() => TransStart("normal-all", "badlogin", "badpass"));
		}

		[Fact]
		public async Task transaction_to_all_access_normal_stream_succeeds_when_any_existing_user_credentials_are_passed() {
			var t = await TransStart("normal-all", "user1", "pa$$1");
			await t.WriteAsync(CreateEvents());
			await t.CommitAsync();
		}

		[Fact]
		public void transaction_to_all_access_normal_stream_succeeds_when_admin_user_credentials_are_passed() {
			ExpectNoException(async () => {
				var t = await TransStart("normal-all", "adm", "admpa$$");
                await t.WriteAsync(CreateEvents());
                await t.CommitAsync();
			});
		}
	}
}
