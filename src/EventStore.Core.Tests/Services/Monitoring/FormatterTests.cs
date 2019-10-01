using EventStore.Common.Utils;
using Xunit;

namespace EventStore.Core.Tests.Services.Monitoring {
	public class FormatterTests {
		[Theory]
		[InlineData(0L, "0B")]
		[InlineData(500L, "500B")]
		[InlineData(1023L, "1023B")]
		[InlineData(1024L, "1KiB")]
		[InlineData(2560L, "2.5KiB")]
		[InlineData(1048576L, "1MiB")]
		[InlineData(502792192L, "479.5MiB")]
		[InlineData(1073741824L, "1GiB")]
		[InlineData(79725330432L, "74.25GiB")]
		[InlineData(1099511627776L, "1TiB")]
		[InlineData(1125899906842624L, "1024TiB")]
		[InlineData(long.MaxValue, "8388608TiB")]
		[InlineData(-1L, "-1B")]
		[InlineData(-1023L, "-1023B")]
		[InlineData(-1024, "-1KiB")]
		[InlineData(-1048576L, "-1MiB")]
		public void test_size_multiple_cases_long(long bytes, string expected) {
			Assert.Equal(expected, bytes.ToFriendlySizeString());
		}

		[Theory]
		[InlineData(0UL, "0B")]
		[InlineData(500UL, "500B")]
		[InlineData(1023UL, "1023B")]
		[InlineData(1024UL, "1KiB")]
		[InlineData(2560UL, "2.5KiB")]
		[InlineData(1048576UL, "1MiB")]
		[InlineData(502792192UL, "479.5MiB")]
		[InlineData(1073741824UL, "1GiB")]
		[InlineData(79725330432UL, "74.25GiB")]
		[InlineData(1099511627776UL, "1TiB")]
		[InlineData(1125899906842624UL, "1024TiB")]
		[InlineData(ulong.MaxValue, "more than long.MaxValue")] //16777215TiB
		public void test_size_multiple_cases_ulong(ulong bytes, string expected) {
			Assert.Equal(expected, bytes.ToFriendlySizeString());
		}


		[Theory]
		[InlineData(0D, "0B/s")]
		[InlineData(500L, "500B/s")]
		[InlineData(1023L, "1023B/s")]
		[InlineData(1024L, "1KiB/s")]
		[InlineData(2560L, "2.5KiB/s")]
		[InlineData(1048576L, "1MiB/s")]
		[InlineData(502792192L, "479.5MiB/s")]
		[InlineData(1073741824L, "1GiB/s")]
		[InlineData(79725330432L, "74.25GiB/s")]
		[InlineData(-1L, "-1B/s")]
		[InlineData(-1023L, "-1023B/s")]
		[InlineData(-1024, "-1KiB/s")]
		[InlineData(-1048576L, "-1MiB/s")]
		public void test_speed_multiple_cases(double speed, string expected) {
			Assert.Equal(expected, speed.ToFriendlySpeedString());
		}


		[Theory]
		[InlineData(0L, "0")]
		[InlineData(500L, "500")]
		[InlineData(1023L, "1023")]
		[InlineData(1024L, "1K")]
		[InlineData(2560L, "2.5K")]
		[InlineData(1048576L, "1M")]
		[InlineData(502792192L, "479.5M")]
		[InlineData(1073741824L, "1G")]
		[InlineData(79725330432L, "74.25G")]
		[InlineData(1099511627776L, "1T")]
		[InlineData(1125899906842624L, "1024T")]
		[InlineData(long.MaxValue, "8388608T")]
		[InlineData(-1L, "-1")]
		[InlineData(-1023L, "-1023")]
		[InlineData(-1024, "-1K")]
		[InlineData(-1048576L, "-1M")]
		public void test_number_multiple_cases_long(long number, string expected) {
			Assert.Equal(expected, number.ToFriendlyNumberString());
		}

		[Theory]
		[InlineData(0UL, "0")]
		[InlineData(500UL, "500")]
		[InlineData(1023UL, "1023")]
		[InlineData(1024UL, "1K")]
		[InlineData(2560UL, "2.5K")]
		[InlineData(1048576UL, "1M")]
		[InlineData(502792192UL, "479.5M")]
		[InlineData(1073741824UL, "1G")]
		[InlineData(79725330432UL, "74.25G")]
		[InlineData(1099511627776UL, "1T")]
		[InlineData(1125899906842624UL, "1024T")]
		[InlineData(ulong.MaxValue, "more than long.MaxValue")] //16777215TiB
		public void test_number_multiple_cases_ulong(ulong number, string expected) {
			Assert.Equal(expected, number.ToFriendlyNumberString());
		}
	}
}
