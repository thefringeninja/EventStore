using System;
using System.Collections.Generic;
using System.Text;
using EventStore.Core.Services.PersistentSubscription;
using Xunit;

namespace EventStore.Core.Tests.Services {
	public class PersistentSubscriptionConfigTests {
		[Fact]
		public void output_can_be_read_as_input_and_keep_same_values() {
			var config = new PersistentSubscriptionConfig();
			config.Updated = new DateTime(2014, 08, 14);
			config.UpdatedBy = "Greg";
			config.Version = "1";
			config.Entries = new List<PersistentSubscriptionEntry>();
			config.Entries.Add(new PersistentSubscriptionEntry()
				{Group = "foo", ResolveLinkTos = true, Stream = "Stream"});
			var data = config.GetSerializedForm();
			var config2 = PersistentSubscriptionConfig.FromSerializedForm(data);
			Assert.Equal(1, config2.Entries.Count);
			Assert.Equal(config.Updated, config2.Updated);
			Assert.Equal(config.UpdatedBy, config2.UpdatedBy);
		}

		[Fact]
		public void bad_json_causes_bad_config_data_exception() {
			var bunkdata = Encoding.UTF8.GetBytes("{'some weird stuff' : 'something'}");
			Assert.Throws<BadConfigDataException>(() => PersistentSubscriptionConfig.FromSerializedForm(bunkdata));
		}

		[Fact]
		public void random_bad_data_causes_bad_config_data_exception() {
			var bunkdata = Encoding.UTF8.GetBytes("This ain't even valid json");
			Assert.Throws<BadConfigDataException>(() => PersistentSubscriptionConfig.FromSerializedForm(bunkdata));
		}
	}
}
