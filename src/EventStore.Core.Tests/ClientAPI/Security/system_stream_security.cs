using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Services;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class system_stream_security : AuthenticationTestBase {
		[Fact]
		public async Task operations_on_system_stream_with_no_acl_set_fail_for_non_admin() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadEvent("$system-no-acl", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamForward("$system-no-acl", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamBackward("$system-no-acl", "user1", "pa$$1"));

			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$system-no-acl", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => TransStart("$system-no-acl", "user1", "pa$$1"));

			var transId = (await TransStart("$system-no-acl", "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user1", "pa$$1"));
			await trans.WriteAsync();
			await Assert.ThrowsAsync<AccessDeniedException>(() => trans.CommitAsync());

			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadMeta("$system-no-acl", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteMeta("$system-no-acl", "user1", "pa$$1", null));

			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToStream("$system-no-acl", "user1", "pa$$1"));
		}

		[Fact]
		public async Task operations_on_system_stream_with_no_acl_set_succeed_for_admin() {
			await ReadEvent("$system-no-acl", "adm", "admpa$$");
			await ReadStreamForward("$system-no-acl", "adm", "admpa$$");
			await ReadStreamBackward("$system-no-acl", "adm", "admpa$$");

			await WriteStream("$system-no-acl", "adm", "admpa$$");
			await TransStart("$system-no-acl", "adm", "admpa$$");

			var transId = (await TransStart("$system-no-acl", "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("adm", "admpa$$"));
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta("$system-no-acl", "adm", "admpa$$");
			await WriteMeta("$system-no-acl", "adm", "admpa$$", null);

			await SubscribeToStream("$system-no-acl", "adm", "admpa$$");
		}

		[Fact]
		public async Task operations_on_system_stream_with_acl_set_to_usual_user_fail_for_not_authorized_user() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadEvent("$system-acl", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamForward("$system-acl", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamBackward("$system-acl", "user2", "pa$$2"));

			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$system-acl", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => TransStart("$system-acl", "user2", "pa$$2"));

			var transId = (await TransStart("$system-acl", "user1", "pa$$1")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user2", "pa$$2"));
			await trans.WriteAsync();
			await Assert.ThrowsAsync<AccessDeniedException>(() => trans.CommitAsync());

			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadMeta("$system-acl", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteMeta("$system-acl", "user2", "pa$$2", "user1"));

			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToStream("$system-acl", "user2", "pa$$2"));
		}

		[Fact]
		public async Task operations_on_system_stream_with_acl_set_to_usual_user_succeed_for_that_user() {
			await ReadEvent("$system-acl", "user1", "pa$$1");
			await ReadStreamForward("$system-acl", "user1", "pa$$1");
			await ReadStreamBackward("$system-acl", "user1", "pa$$1");

			await WriteStream("$system-acl", "user1", "pa$$1");
			await TransStart("$system-acl", "user1", "pa$$1");

			var transId = (await TransStart("$system-acl", "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user1", "pa$$1"));
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta("$system-acl", "user1", "pa$$1");
			await WriteMeta("$system-acl", "user1", "pa$$1", "user1");

			await SubscribeToStream("$system-acl", "user1", "pa$$1");
		}

		[Fact]
		public async Task operations_on_system_stream_with_acl_set_to_usual_user_succeed_for_admin() {
			await ReadEvent("$system-acl", "adm", "admpa$$");
			await ReadStreamForward("$system-acl", "adm", "admpa$$");
			await ReadStreamBackward("$system-acl", "adm", "admpa$$");

			await WriteStream("$system-acl", "adm", "admpa$$");
			await TransStart("$system-acl", "adm", "admpa$$");

			var transId = (await TransStart("$system-acl", "user1", "pa$$1")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("adm", "admpa$$"));
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta("$system-acl", "adm", "admpa$$");
			await WriteMeta("$system-acl", "adm", "admpa$$", "user1");

			await SubscribeToStream("$system-acl", "adm", "admpa$$");
		}


		[Fact]
		public async Task operations_on_system_stream_with_acl_set_to_admins_fail_for_usual_user() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadEvent("$system-adm", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamForward("$system-adm", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamBackward("$system-adm", "user1", "pa$$1"));

			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$system-adm", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => TransStart("$system-adm", "user1", "pa$$1"));

			var transId = (await TransStart("$system-adm", "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user1", "pa$$1"));
			await trans.WriteAsync();
			await Assert.ThrowsAsync<AccessDeniedException>(() => trans.CommitAsync());

			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadMeta("$system-adm", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteMeta("$system-adm", "user1", "pa$$1", SystemRoles.Admins));

			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToStream("$system-adm", "user1", "pa$$1"));
		}

		[Fact]
		public async Task operations_on_system_stream_with_acl_set_to_admins_succeed_for_admin() {
			await ReadEvent("$system-adm", "adm", "admpa$$");
			await ReadStreamForward("$system-adm", "adm", "admpa$$");
			await ReadStreamBackward("$system-adm", "adm", "admpa$$");

			await WriteStream("$system-adm", "adm", "admpa$$");
			await TransStart("$system-adm", "adm", "admpa$$");

			var transId = (await TransStart("$system-adm", "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("adm", "admpa$$"));
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta("$system-adm", "adm", "admpa$$");
			await WriteMeta("$system-adm", "adm", "admpa$$", SystemRoles.Admins);

			await SubscribeToStream("$system-adm", "adm", "admpa$$");
		}


		[Fact]
		public async Task operations_on_system_stream_with_acl_set_to_all_succeed_for_not_authenticated_user() {
			await ReadEvent("$system-all", null, null);
			await ReadStreamForward("$system-all", null, null);
			await ReadStreamBackward("$system-all", null, null);

			await WriteStream("$system-all", null, null);
			await TransStart("$system-all", null, null);

			var transId = (await TransStart("$system-all", null, null)).TransactionId;
			var trans = Connection.ContinueTransaction(transId);
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta("$system-all", null, null);
			await WriteMeta("$system-all", null, null, SystemRoles.All);

			await SubscribeToStream("$system-all", null, null);
		}

		[Fact]
		public async Task operations_on_system_stream_with_acl_set_to_all_succeed_for_usual_user() {
			await ReadEvent("$system-all", "user1", "pa$$1");
			await ReadStreamForward("$system-all", "user1", "pa$$1");
			await ReadStreamBackward("$system-all", "user1", "pa$$1");

			await WriteStream("$system-all", "user1", "pa$$1");
			await TransStart("$system-all", "user1", "pa$$1");

			var transId = (await TransStart("$system-all", "user1", "pa$$1")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user1", "pa$$1"));
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta("$system-all", "user1", "pa$$1");
			await WriteMeta("$system-all", "user1", "pa$$1", SystemRoles.All);

			await SubscribeToStream("$system-all", "user1", "pa$$1");
		}

		[Fact]
		public async Task operations_on_system_stream_with_acl_set_to_all_succeed_for_admin() {
			await ReadEvent("$system-all", "adm", "admpa$$");
			await ReadStreamForward("$system-all", "adm", "admpa$$");
			await ReadStreamBackward("$system-all", "adm", "admpa$$");

			await WriteStream("$system-all", "adm", "admpa$$");
			await TransStart("$system-all", "adm", "admpa$$");

			var transId = (await TransStart("$system-all", "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("adm", "admpa$$"));
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta("$system-all", "adm", "admpa$$");
			await WriteMeta("$system-all", "adm", "admpa$$", SystemRoles.All);

			await SubscribeToStream("$system-all", "adm", "admpa$$");
		}
	}
}
