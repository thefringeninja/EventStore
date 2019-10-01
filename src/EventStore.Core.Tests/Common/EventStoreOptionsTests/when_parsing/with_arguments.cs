using EventStore.Common.Options;
using EventStore.Core.Util;
using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Common.EventStoreOptionsTests.when_parsing {
	public class with_arguments {
		[Fact]
		public void should_use_the_supplied_argument() {
			var args = new string[] {"-log", "~/customLogDirectory"};
			var testArgs = EventStoreOptions.Parse<TestArgs>(args, Opts.EnvPrefix);
			Assert.Equal("~/customLogDirectory", testArgs.Log);
		}
	}
}
