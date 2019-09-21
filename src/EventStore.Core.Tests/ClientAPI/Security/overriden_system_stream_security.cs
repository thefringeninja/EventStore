﻿using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[TestFixture, Category("ClientAPI"), Category("LongRunning"), Category("Network")]
	public class overriden_system_stream_security : AuthenticationTestBase {
		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

			var settings = new SystemSettings(userStreamAcl: null,
				systemStreamAcl: new StreamAcl("user1", "user1", "user1", "user1", "user1"));
            await Connection.SetSystemSettingsAsync(settings, new UserCredentials("adm", "admpa$$"));
		}

		[Test]
		public async Task operations_on_system_stream_succeed_for_authorized_user() {
			const string stream = "$sys-authorized-user";
			await ReadEvent(stream, "user1", "pa$$1");
			await ReadStreamForward(stream, "user1", "pa$$1");
			await ReadStreamBackward(stream, "user1", "pa$$1");

			await WriteStream(stream, "user1", "pa$$1");
			await TransStart(stream, "user1", "pa$$1");

			var transId = (await TransStart(stream, "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user1", "pa$$1"));
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta(stream, "user1", "pa$$1");
			await WriteMeta(stream, "user1", "pa$$1", null);

			await SubscribeToStream(stream, "user1", "pa$$1");

			await DeleteStream(stream, "user1", "pa$$1");
		}

		[Test]
		public async Task operations_on_system_stream_fail_for_not_authorized_user() {
			const string stream = "$sys-not-authorized-user";
			Expect<AccessDeniedException>(() => ReadEvent(stream, "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => ReadStreamForward(stream, "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => ReadStreamBackward(stream, "user2", "pa$$2"));

			Expect<AccessDeniedException>(() => WriteStream(stream, "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => TransStart(stream, "user2", "pa$$2"));

			var transId = (await TransStart(stream, "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("user2", "pa$$2"));
			await trans.WriteAsync();
			Expect<AccessDeniedException>(() => trans.CommitAsync());

			Expect<AccessDeniedException>(() => ReadMeta(stream, "user2", "pa$$2"));
			Expect<AccessDeniedException>(() => WriteMeta(stream, "user2", "pa$$2", null));

			Expect<AccessDeniedException>(() => SubscribeToStream(stream, "user2", "pa$$2"));

			Expect<AccessDeniedException>(() => DeleteStream(stream, "user2", "pa$$2"));
		}

		[Test]
		public async Task operations_on_system_stream_fail_for_anonymous_user() {
			const string stream = "$sys-anonymous-user";
			Expect<AccessDeniedException>(() => ReadEvent(stream, null, null));
			Expect<AccessDeniedException>(() => ReadStreamForward(stream, null, null));
			Expect<AccessDeniedException>(() => ReadStreamBackward(stream, null, null));

			Expect<AccessDeniedException>(() => WriteStream(stream, null, null));
			Expect<AccessDeniedException>(() => TransStart(stream, null, null));

			var transId = (await TransStart(stream, "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId);
			await trans.WriteAsync();
			Expect<AccessDeniedException>(() => trans.CommitAsync());

			Expect<AccessDeniedException>(() => ReadMeta(stream, null, null));
			Expect<AccessDeniedException>(() => WriteMeta(stream, null, null, null));

			Expect<AccessDeniedException>(() => SubscribeToStream(stream, null, null));

			Expect<AccessDeniedException>(() => DeleteStream(stream, null, null));
		}

		[Test]
		public async Task operations_on_system_stream_succeed_for_admin() {
			const string stream = "$sys-admin";
			await ReadEvent(stream, "adm", "admpa$$");
			await ReadStreamForward(stream, "adm", "admpa$$");
			await ReadStreamBackward(stream, "adm", "admpa$$");

			await WriteStream(stream, "adm", "admpa$$");
			await TransStart(stream, "adm", "admpa$$");

			var transId = (await TransStart(stream, "adm", "admpa$$")).TransactionId;
			var trans = Connection.ContinueTransaction(transId, new UserCredentials("adm", "admpa$$"));
			await trans.WriteAsync();
			await trans.CommitAsync();

			await ReadMeta(stream, "adm", "admpa$$");
			await WriteMeta(stream, "adm", "admpa$$", null);

			await SubscribeToStream(stream, "adm", "admpa$$");

			await DeleteStream(stream, "adm", "admpa$$");
		}
	}
}
