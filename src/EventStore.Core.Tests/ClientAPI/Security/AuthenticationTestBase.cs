using System;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services;
using EventStore.Core.Services.UserManagement;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI.Security {
	public abstract class AuthenticationTestBase : SpecificationWithDirectoryPerTestFixture {
		private readonly UserCredentials _userCredentials;
		private MiniNode _node;
		protected IEventStoreConnection Connection;

		protected AuthenticationTestBase(UserCredentials userCredentials = null) {
			_userCredentials = userCredentials;
		}

		public virtual IEventStoreConnection SetupConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint, TcpType.Normal, _userCredentials);
		}

		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName, enableTrustedAuth: true);
			try {
				_node.Start();

				var userCreateEvent1 = new TaskCompletionSource<bool>();
				_node.Node.MainQueue.Publish(
					new UserManagementMessage.Create(
						new CallbackEnvelope(
							m => {
								Assert.IsTrue(m is UserManagementMessage.UpdateResult);
								var msg = (UserManagementMessage.UpdateResult)m;
								Assert.IsTrue(msg.Success);

								userCreateEvent1.SetResult(true);
							}),
						SystemAccount.Principal,
						"user1",
						"Test User 1",
						new string[0],
						"pa$$1"));

				var userCreateEvent2 = new TaskCompletionSource<bool>();
				_node.Node.MainQueue.Publish(
					new UserManagementMessage.Create(
						new CallbackEnvelope(
							m => {
								Assert.IsTrue(m is UserManagementMessage.UpdateResult);
								var msg = (UserManagementMessage.UpdateResult)m;
								Assert.IsTrue(msg.Success);

								userCreateEvent2.SetResult(true);
							}),
						SystemAccount.Principal,
						"user2",
						"Test User 2",
						new string[0],
						"pa$$2"));

				var adminCreateEvent2 = new TaskCompletionSource<bool>();
				_node.Node.MainQueue.Publish(
					new UserManagementMessage.Create(
						new CallbackEnvelope(
							m => {
								Assert.IsTrue(m is UserManagementMessage.UpdateResult);
								var msg = (UserManagementMessage.UpdateResult)m;
								Assert.IsTrue(msg.Success);

								adminCreateEvent2.SetResult(true);
							}),
						SystemAccount.Principal,
						"adm",
						"Administrator User",
						new[] {SystemRoles.Admins},
						"admpa$$"));

				Assert.IsTrue(await userCreateEvent1.Task.WithTimeout(10000), "User 1 creation failed");
				Assert.IsTrue(await userCreateEvent2.Task.WithTimeout(10000), "User 2 creation failed");
				Assert.IsTrue(await adminCreateEvent2.Task.WithTimeout(10000), "Administrator User creation failed");

				Connection = SetupConnection(_node);
				await Connection.ConnectAsync();

				await Connection.SetStreamMetadataAsync("noacl-stream", ExpectedVersion.NoStream, StreamMetadata.Build());
				await Connection.SetStreamMetadataAsync(
					"read-stream",
					ExpectedVersion.NoStream,
					StreamMetadata.Build().SetReadRole("user1"));
				await Connection.SetStreamMetadataAsync(
					"write-stream",
					ExpectedVersion.NoStream,
					StreamMetadata.Build().SetWriteRole("user1"));
				await Connection.SetStreamMetadataAsync(
					"metaread-stream",
					ExpectedVersion.NoStream,
					StreamMetadata.Build().SetMetadataReadRole("user1"));
				await Connection.SetStreamMetadataAsync(
					"metawrite-stream",
					ExpectedVersion.NoStream,
					StreamMetadata.Build().SetMetadataWriteRole("user1"));

				await Connection.SetStreamMetadataAsync(
					"$all",
					ExpectedVersion.Any,
					StreamMetadata.Build().SetReadRole("user1"),
					new UserCredentials("adm", "admpa$$"));

				await Connection.SetStreamMetadataAsync(
					"$system-acl",
					ExpectedVersion.NoStream,
					StreamMetadata.Build()
						.SetReadRole("user1")
						.SetWriteRole("user1")
						.SetMetadataReadRole("user1")
						.SetMetadataWriteRole("user1"),
					new UserCredentials("adm", "admpa$$"));
				await Connection.SetStreamMetadataAsync(
					"$system-adm",
					ExpectedVersion.NoStream,
					StreamMetadata.Build()
						.SetReadRole(SystemRoles.Admins)
						.SetWriteRole(SystemRoles.Admins)
						.SetMetadataReadRole(SystemRoles.Admins)
						.SetMetadataWriteRole(SystemRoles.Admins),
					new UserCredentials("adm", "admpa$$"));

				await Connection.SetStreamMetadataAsync(
					"normal-all",
					ExpectedVersion.NoStream,
					StreamMetadata.Build()
						.SetReadRole(SystemRoles.All)
						.SetWriteRole(SystemRoles.All)
						.SetMetadataReadRole(SystemRoles.All)
						.SetMetadataWriteRole(SystemRoles.All));
				await Connection.SetStreamMetadataAsync(
					"$system-all",
					ExpectedVersion.NoStream,
					StreamMetadata.Build()
						.SetReadRole(SystemRoles.All)
						.SetWriteRole(SystemRoles.All)
						.SetMetadataReadRole(SystemRoles.All)
						.SetMetadataWriteRole(SystemRoles.All),
					new UserCredentials("adm", "admpa$$"));
			} catch {
				if (_node != null)
					try {
						_node.Shutdown();
					} catch {
					}

				throw;
			}
		}

		[OneTimeTearDown]
		public override Task TestFixtureTearDown() {
			_node.Shutdown();
			Connection.Close();
			return base.TestFixtureTearDown();
		}

		protected void ReadEvent(string streamId, string login, string password) {
			Connection.ReadEventAsync(streamId, -1, false,
					login == null && password == null ? null : new UserCredentials(login, password))
				.Wait();
		}

		protected void ReadStreamForward(string streamId, string login, string password) {
			Connection.ReadStreamEventsForwardAsync(streamId, 0, 1, false,
					login == null && password == null ? null : new UserCredentials(login, password))
				.Wait();
		}

		protected void ReadStreamBackward(string streamId, string login, string password) {
			Connection.ReadStreamEventsBackwardAsync(streamId, 0, 1, false,
					login == null && password == null ? null : new UserCredentials(login, password))
				.Wait();
		}

		protected void WriteStream(string streamId, string login, string password) {
			Connection.AppendToStreamAsync(streamId, ExpectedVersion.Any, CreateEvents(),
					login == null && password == null ? null : new UserCredentials(login, password))
				.Wait();
		}

		protected EventStoreTransaction TransStart(string streamId, string login, string password) {
			return Connection.StartTransactionAsync(streamId, ExpectedVersion.Any,
					login == null && password == null ? null : new UserCredentials(login, password))
				.Result;
		}

		protected void ReadAllForward(string login, string password) {
			Connection.ReadAllEventsForwardAsync(Position.Start, 1, false,
					login == null && password == null ? null : new UserCredentials(login, password))
				.Wait();
		}

		protected void ReadAllBackward(string login, string password) {
			Connection.ReadAllEventsBackwardAsync(Position.End, 1, false,
					login == null && password == null ? null : new UserCredentials(login, password))
				.Wait();
		}

		protected void ReadMeta(string streamId, string login, string password) {
			Connection.GetStreamMetadataAsRawBytesAsync(streamId,
				login == null && password == null ? null : new UserCredentials(login, password)).Wait();
		}

		protected void WriteMeta(string streamId, string login, string password, string metawriteRole) {
			Connection.SetStreamMetadataAsync(streamId, ExpectedVersion.Any,
					metawriteRole == null
						? StreamMetadata.Build()
						: StreamMetadata.Build().SetReadRole(metawriteRole)
							.SetWriteRole(metawriteRole)
							.SetMetadataReadRole(metawriteRole)
							.SetMetadataWriteRole(metawriteRole),
					login == null && password == null ? null : new UserCredentials(login, password))
				.Wait();
		}

		protected void SubscribeToStream(string streamId, string login, string password) {
			using (Connection.SubscribeToStreamAsync(streamId, false, (x, y) => Task.CompletedTask, (x, y, z) => { },
				login == null && password == null ? null : new UserCredentials(login, password)).Result) {
			}
		}

		protected void SubscribeToAll(string login, string password) {
			using (Connection.SubscribeToAllAsync(false, (x, y) => Task.CompletedTask, (x, y, z) => { },
				login == null && password == null ? null : new UserCredentials(login, password)).Result) {
			}
		}

		protected string CreateStreamWithMeta(StreamMetadata metadata, string streamPrefix = null) {
			var stream = (streamPrefix ?? string.Empty) + TestContext.CurrentContext.Test.Name;
			Connection.SetStreamMetadataAsync(stream, ExpectedVersion.NoStream,
				metadata, new UserCredentials("adm", "admpa$$")).Wait();
			return stream;
		}

		protected void DeleteStream(string streamId, string login, string password) {
			Connection.DeleteStreamAsync(streamId, ExpectedVersion.Any, true,
				login == null && password == null ? null : new UserCredentials(login, password)).Wait();
		}

		protected void Expect<T>(Action action) where T : Exception {
			Assert.That(() => action(),
				Throws.Exception.InstanceOf<AggregateException>().With.InnerException.InstanceOf<T>());
		}

		protected void ExpectNoException(Action action) {
			Assert.That(() => action(), Throws.Nothing);
		}

		protected EventData[] CreateEvents() {
			return new[] {new EventData(Guid.NewGuid(), "some-type", false, new byte[] {1, 2, 3}, null)};
		}
	}
}
