using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[TestFixture, Category("ClientAPI"), Category("LongRunning"), Category("Network")]
	public class authorized_default_credentials_security : AuthenticationTestBase {
		public authorized_default_credentials_security() : base(new UserCredentials("user1", "pa$$1")) {
		}

		[Test]
		public async Task all_operations_succeeds_when_passing_no_explicit_credentials() {
			await ReadAllForward(null, null);
			await ReadAllBackward(null, null);

			await ReadEvent("read-stream", null, null);
			await ReadStreamForward("read-stream", null, null);
			await ReadStreamBackward("read-stream", null, null);

			await WriteStream("write-stream", null, null);
			await ExpectNoException(async () => {
				var trans = await TransStart("write-stream", null, null);
				await trans.WriteAsync();
				await trans.CommitAsync();
			});

			await ReadMeta("metaread-stream", null, null);
			await WriteMeta("metawrite-stream", null, null, "user1");

			await SubscribeToStream("read-stream", null, null);
			await SubscribeToAll(null, null);
		}

		[Test]
		public async Task all_operations_are_not_authenticated_when_overriden_with_not_existing_credentials() {
			Expect<NotAuthenticatedException>(() => ReadAllForward("badlogin", "badpass"));
			Expect<NotAuthenticatedException>(() => ReadAllBackward("badlogin", "badpass"));

			Expect<NotAuthenticatedException>(() => ReadEvent("read-stream", "badlogin", "badpass"));
			Expect<NotAuthenticatedException>(() => ReadStreamForward("read-stream", "badlogin", "badpass"));
			Expect<NotAuthenticatedException>(() => ReadStreamBackward("read-stream", "badlogin", "badpass"));

			Expect<NotAuthenticatedException>(() => WriteStream("write-stream", "badlogin", "badpass"));
			Expect<NotAuthenticatedException>(() => TransStart("write-stream", "badlogin", "badpass"));

			var transId = (await TransStart("write-stream", null, null)).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("badlogin", "badpass"));
			await trans.WriteAsync();
			Expect<NotAuthenticatedException>(() => trans.CommitAsync());

			Expect<NotAuthenticatedException>(() => ReadMeta("metaread-stream", "badlogin", "badpass"));
			Expect<NotAuthenticatedException>(() => WriteMeta("metawrite-stream", "badlogin", "badpass", "user1"));

			Expect<NotAuthenticatedException>(() => SubscribeToStream("read-stream", "badlogin", "badpass"));
			Expect<NotAuthenticatedException>(() => SubscribeToAll("badlogin", "badpass"));
		}

		[Test]
		public async Task all_operations_are_not_authorized_when_overriden_with_not_authorized_credentials() {
			Expect<AccessDeniedException>(() => ReadAllForward("user2", "pa$$2"));
			Expect<AccessDeniedException>(() => ReadAllBackward("user2", "pa$$2"));

			Expect<AccessDeniedException>(() => ReadEvent("read-stream", "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => ReadStreamForward("read-stream", "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => ReadStreamBackward("read-stream", "user2", "pa$$2"));

			Expect<AccessDeniedException>(() => WriteStream("write-stream", "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => TransStart("write-stream", "user2", "pa$$2"));

			var transId = (await TransStart("write-stream", null, null)).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user2", "pa$$2"));
			await trans.WriteAsync();
			Expect<AccessDeniedException>(() => trans.CommitAsync());

			Expect<AccessDeniedException>(() => ReadMeta("metaread-stream", "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => WriteMeta("metawrite-stream", "user2", "pa$$2", "user1"));

			Expect<AccessDeniedException>(() => SubscribeToStream("read-stream", "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => SubscribeToAll("user2", "pa$$2"));
		}
	}
}
