using EventStore.ClientAPI;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI")]
	public class connection_string {
		[Fact]
		public void can_set_string_value() {
			var settings = ConnectionString.GetConnectionSettings("targethost=testtest");
			Assert.Equal("testtest", settings.TargetHost);
		}

		[Fact]
		public void can_set_bool_value_with_string() {
			var settings = ConnectionString.GetConnectionSettings("verboselogging=true");
			Assert.Equal(true, settings.VerboseLogging);
		}

		[Fact]
		public void can_set_with_spaces() {
			var settings = ConnectionString.GetConnectionSettings("Verbose Logging=true");
			Assert.Equal(true, settings.VerboseLogging);
		}


		[Fact]
		public void can_set_int() {
			var settings = ConnectionString.GetConnectionSettings("maxretries=55");
			Assert.Equal(55, settings.MaxRetries);
		}

		[Fact]
		public void can_set_timespan() {
			var settings = ConnectionString.GetConnectionSettings("heartbeattimeout=5555");
			Assert.Equal(5555, settings.HeartbeatTimeout.TotalMilliseconds);
		}

		[Fact]
		public void can_set_multiple_values() {
			var settings = ConnectionString.GetConnectionSettings("heartbeattimeout=5555;maxretries=55");
			Assert.Equal(5555, settings.HeartbeatTimeout.TotalMilliseconds);
			Assert.Equal(55, settings.MaxRetries);
		}

		[Fact]
		public void can_set_mixed_case() {
			var settings = ConnectionString.GetConnectionSettings("heArtbeAtTimeout=5555");
			Assert.Equal(5555, settings.HeartbeatTimeout.TotalMilliseconds);
		}

		[Fact]
		public void can_set_gossip_seeds() {
			var settings =
				ConnectionString.GetConnectionSettings(
					"gossipseeds=111.222.222.111:1111,111.222.222.111:1112,111.222.222.111:1113");
			Assert.Equal(3, settings.GossipSeeds.Length);
		}

		[Fact]
		public void can_set_default_user_credentials() {
			var settings = ConnectionString.GetConnectionSettings("DefaultUserCredentials=foo:bar");
			Assert.Equal("foo", settings.DefaultUserCredentials.Username);
			Assert.Equal("bar", settings.DefaultUserCredentials.Password);
		}
	}
}
