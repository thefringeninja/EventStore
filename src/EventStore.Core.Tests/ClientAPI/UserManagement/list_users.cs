using EventStore.ClientAPI.SystemData;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class list_users : TestWithNode {
		[Fact]
		public async System.Threading.Tasks.Task list_all_users_worksAsync() {
            await _manager.CreateUserAsync("ouro", "ourofull", new[] {"foo", "bar"}, "ouro",
				new UserCredentials("admin", "changeit"));
			var x = await _manager.ListAllAsync(new UserCredentials("admin", "changeit"));
			Assert.Equal(3, x.Count);
			Assert.Equal("admin", x[0].LoginName);
			Assert.Equal("Event Store Administrator", x[0].FullName);
			Assert.Equal("ops", x[1].LoginName);
			Assert.Equal("Event Store Operations", x[1].FullName);
			Assert.Equal("ouro", x[2].LoginName);
			Assert.Equal("ourofull", x[2].FullName);
		}
	}
}
