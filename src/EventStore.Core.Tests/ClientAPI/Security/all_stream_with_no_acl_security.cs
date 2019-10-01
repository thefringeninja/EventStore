using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class all_stream_with_no_acl_security : AuthenticationTestBase {
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();

            await Connection.SetStreamMetadataAsync("$all", ExpectedVersion.Any, StreamMetadata.Build(),
				new UserCredentials("adm", "admpa$$"));
		}

		[Fact]
		public async Task write_to_all_is_never_allowed() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$all", null, null));
			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$all", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => WriteStream("$all", "adm", "admpa$$"));
		}

		[Fact]
		public async Task delete_of_all_is_never_allowed() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => DeleteStream("$all", null, null));
			await Assert.ThrowsAsync<AccessDeniedException>(() => DeleteStream("$all", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => DeleteStream("$all", "adm", "admpa$$"));
		}


		[Fact]
		public async Task reading_and_subscribing_is_not_allowed_when_no_credentials_are_passed() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadEvent("$all", null, null));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamForward("$all", null, null));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamBackward("$all", null, null));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadMeta("$all", null, null));
			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToStream("$all", null, null));
		}

		[Fact]
		public async Task reading_and_subscribing_is_not_allowed_for_usual_user() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadEvent("$all", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamForward("$all", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadStreamBackward("$all", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadMeta("$all", "user1", "pa$$1"));
			await Assert.ThrowsAsync<AccessDeniedException>(() => SubscribeToStream("$all", "user1", "pa$$1"));
		}

		[Fact]
		public async Task reading_and_subscribing_is_allowed_for_admin_user() {
			await ReadEvent("$all", "adm", "admpa$$");
			await ReadStreamForward("$all", "adm", "admpa$$");
			await ReadStreamBackward("$all", "adm", "admpa$$");
			await ReadMeta("$all", "adm", "admpa$$");
			await SubscribeToStream("$all", "adm", "admpa$$");
		}


		[Fact]
		public Task meta_write_is_not_allowed_when_no_credentials_are_passed() {
			return Assert.ThrowsAsync<AccessDeniedException>(() => WriteMeta("$all", null, null, null));
		}

		[Fact]
		public Task meta_write_is_not_allowed_for_usual_user() {
			return Assert.ThrowsAsync<AccessDeniedException>(() => WriteMeta("$all", "user1", "pa$$1", null));
		}

		[Fact]
		public async Task meta_write_is_allowed_for_admin_user() {
			await WriteMeta("$all", "adm", "admpa$$", null);
		}
	}
}
