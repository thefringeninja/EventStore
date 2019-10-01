﻿using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class authorized_default_credentials_security : AuthenticationTestBase {
		public authorized_default_credentials_security() : base(new UserCredentials("user1", "pa$$1")) {
		}

		[Fact]
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

		[Fact]
		public async Task all_operations_are_not_authenticated_when_overriden_with_not_existing_credentials() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadAllForward("badlogin", "badpass"));
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadAllBackward("badlogin", "badpass"));

			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadEvent("read-stream", "badlogin", "badpass"));
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadStreamForward("read-stream", "badlogin", "badpass"));
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadStreamBackward("read-stream", "badlogin", "badpass"));

			await Assert.ThrowsAsync<NotAuthenticatedException>(() => WriteStream("write-stream", "badlogin", "badpass"));
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => TransStart("write-stream", "badlogin", "badpass"));

			var transId = (await TransStart("write-stream", null, null)).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("badlogin", "badpass"));
			await trans.WriteAsync();
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => trans.CommitAsync());

			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadMeta("metaread-stream", "badlogin", "badpass"));
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => WriteMeta("metawrite-stream", "badlogin", "badpass", "user1"));

			await Assert.ThrowsAsync<NotAuthenticatedException>(() => SubscribeToStream("read-stream", "badlogin", "badpass"));
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => SubscribeToAll("badlogin", "badpass"));
		}

		[Fact]
		public async Task all_operations_are_not_authorized_when_overriden_with_not_authorized_credentials() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadAllForward("user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadAllBackward("user2", "pa$$2"));

			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadEvent("read-stream", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamForward("read-stream", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamBackward("read-stream", "user2", "pa$$2"));

			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("write-stream", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => TransStart("write-stream", "user2", "pa$$2"));

			var transId = (await TransStart("write-stream", null, null)).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user2", "pa$$2"));
			await trans.WriteAsync();
			await Assert.ThrowsAsync<AccessDeniedException>(() => trans.CommitAsync());

			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadMeta("metaread-stream", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteMeta("metawrite-stream", "user2", "pa$$2", "user1"));

			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToStream("read-stream", "user2", "pa$$2"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToAll("user2", "pa$$2"));
		}
	}
}
