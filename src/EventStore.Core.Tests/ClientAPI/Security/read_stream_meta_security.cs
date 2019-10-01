﻿using System.Threading.Tasks;
using EventStore.ClientAPI.Exceptions;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Security {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning"), Trait("Category", "Network")]
	public class read_stream_meta_security : AuthenticationTestBase {
		[Fact]
		public async Task reading_stream_meta_with_not_existing_credentials_is_not_authenticated() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadMeta("metaread-stream", "badlogin", "badpass"));
		}

		[Fact]
		public async Task reading_stream_meta_with_no_credentials_is_denied() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadMeta("metaread-stream", null, null));
		}

		[Fact]
		public async Task reading_stream_meta_with_not_authorized_user_credentials_is_denied() {
			await Assert.ThrowsAsync<AccessDeniedException>(() => ReadMeta("metaread-stream", "user2", "pa$$2"));
		}

		[Fact]
		public async Task reading_stream_meta_with_authorized_user_credentials_succeeds() {
			await ReadMeta("metaread-stream", "user1", "pa$$1");
		}

		[Fact]
		public async Task reading_stream_meta_with_admin_user_credentials_succeeds() {
			await ReadMeta("metaread-stream", "adm", "admpa$$");
		}


		[Fact]
		public async Task reading_no_acl_stream_meta_succeeds_when_no_credentials_are_passed() {
			await ReadMeta("noacl-stream", null, null);
		}

		[Fact]
		public async Task reading_no_acl_stream_meta_is_not_authenticated_when_not_existing_credentials_are_passed() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadMeta("noacl-stream", "badlogin", "badpass"));
		}

		[Fact]
		public async Task reading_no_acl_stream_meta_succeeds_when_any_existing_user_credentials_are_passed() {
			await ReadMeta("noacl-stream", "user1", "pa$$1");
			await ReadMeta("noacl-stream", "user2", "pa$$2");
		}

		[Fact]
		public async Task reading_no_acl_stream_meta_succeeds_when_admin_user_credentials_are_passed() {
			await ReadMeta("noacl-stream", "adm", "admpa$$");
		}


		[Fact]
		public async Task reading_all_access_normal_stream_meta_succeeds_when_no_credentials_are_passed() {
			await ReadMeta("normal-all", null, null);
		}

		[Fact]
		public async Task
			reading_all_access_normal_stream_meta_is_not_authenticated_when_not_existing_credentials_are_passed() {
			await Assert.ThrowsAsync<NotAuthenticatedException>(() => ReadMeta("normal-all", "badlogin", "badpass"));
		}

		[Fact]
		public async Task reading_all_access_normal_stream_meta_succeeds_when_any_existing_user_credentials_are_passed() {
			await ReadMeta("normal-all", "user1", "pa$$1");
			await ReadMeta("normal-all", "user2", "pa$$2");
		}

		[Fact]
		public async Task reading_all_access_normal_stream_meta_succeeds_when_admin_user_credentials_are_passed() {
			await ReadMeta("normal-all", "adm", "admpa$$");
		}
	}
}
