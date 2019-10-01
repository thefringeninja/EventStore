using System.Threading.Tasks;
using EventStore.ClientAPI.SystemData;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.UserManagement {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class get_current_user : TestWithNode {
		[Fact]
		public async Task returns_the_current_user() {
			var x = await _manager.GetCurrentUserAsync(new UserCredentials("admin", "changeit"));
			Assert.Equal("admin", x.LoginName);
			Assert.Equal("Event Store Administrator", x.FullName);
		}
	}
}
