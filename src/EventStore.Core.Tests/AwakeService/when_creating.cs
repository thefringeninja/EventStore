using Xunit;

namespace EventStore.Core.Tests.AwakeService {
	public class when_creating {
		[Fact]
		public void it_can_ce_created() {
			var it = new Core.Services.AwakeReaderService.AwakeService();
			Assert.NotNull(it);
		}
	}
}
