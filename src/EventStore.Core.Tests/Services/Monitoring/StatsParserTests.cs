using System;
using EventStore.Core.Services.Monitoring.Stats;
using EventStore.Core.Tests.Fakes;
using Xunit;

namespace EventStore.Core.Tests.Services.Monitoring {
	public class IoParserTests {
		private readonly string ioStr = "rchar: 23550615" + Environment.NewLine +
		                                "wchar: 290654" + Environment.NewLine +
		                                "syscr: 184391" + Environment.NewLine +
		                                "syscw: 3273" + Environment.NewLine +
		                                "read_bytes: 13824000" + Environment.NewLine +
		                                "write_bytes: 188416" + Environment.NewLine +
		                                "cancelled_write_bytes: 0" + Environment.NewLine;

		[Fact]
		public void sample_io_doesnt_crash() {
			var io = DiskIo.ParseOnUnix(ioStr, new FakeLogger());
			var success = io != null;

			Assert.True(success);;
		}

		[Fact]
		public void bad_io_crashes() {
			var badIoStr = ioStr.Remove(5, 20);

			DiskIo io = DiskIo.ParseOnUnix(badIoStr, new FakeLogger());
			var success = io != null;

			Assert.False(success);
		}

		[Fact]
		public void read_bytes_parses_ok() {
			var io = DiskIo.ParseOnUnix(ioStr, new FakeLogger());

			Assert.Equal(io.ReadBytes, 13824000UL);
		}

		[Fact]
		public void write_bytes_parses_ok() {
			var io = DiskIo.ParseOnUnix(ioStr, new FakeLogger());

			Assert.Equal(io.WrittenBytes, 188416UL);
		}
	}
}
