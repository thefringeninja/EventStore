using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using Xunit;
using EventStore.Core.Tests.ClientAPI;
using EventStore.Core.Tests.Helpers;
using HttpStatusCode = System.Net.HttpStatusCode;

namespace EventStore.Core.Tests.Services.Transport.Http {
	public class when_getting_tcp_stats_from_stat_controller : SpecificationWithMiniNode {
		private PortableServer _portableServer;
		private IPEndPoint _serverEndPoint;
		private int _serverPort;
		private IEventStoreConnection _connection;
		private string _url;
		private string _clientConnectionName = "test-connection";

		private List<MonitoringMessage.TcpConnectionStats> _results = new List<MonitoringMessage.TcpConnectionStats>();
		private HttpResponse _response;

		protected override async Task Given() {
			_serverPort = PortsHelper.GetAvailablePort(IPAddress.Loopback);
			_serverEndPoint = new IPEndPoint(IPAddress.Loopback, _serverPort);
			_url = _HttpEndPoint.ToHttpUrl(EndpointExtensions.HTTP_SCHEMA, "/stats/tcp");

			var settings = ConnectionSettings.Create();
			_connection = EventStoreConnection.Create(settings, _node.TcpEndPoint, _clientConnectionName);
            await _connection.ConnectAsync();

			var testEvent = new EventData(Guid.NewGuid(), "TestEvent", true,
				Encoding.ASCII.GetBytes("{'Test' : 'OneTwoThree'}"), null);
            await _connection.AppendToStreamAsync("tests", ExpectedVersion.Any, testEvent);

			_portableServer = new PortableServer(_serverEndPoint);
			_portableServer.SetUp();
		}

		protected override Task When() {
			Func<HttpResponse, bool> verifier = response => {
				_results = Codec.Json.From<List<MonitoringMessage.TcpConnectionStats>>(response.Body);
				_response = response;
				return true;
			};

			var res = _portableServer.StartServiceAndSendRequest(y => { }, _url, verifier);
			Assert.Empty(res.Item2);
			return Task.CompletedTask;
		}

		[Fact]
		public void should_have_succeeded() {
			Assert.Equal((int)HttpStatusCode.OK, _response.HttpStatusCode);
		}

		[Fact]
		public void should_return_the_external_connections() {
			Assert.Equal(2, _results.Count(r => r.IsExternalConnection));
		}

		[Fact]
		public void should_return_the_total_number_of_bytes_sent_for_external_connections() {
			Assert.True(_results.Sum(r => r.IsExternalConnection ? r.TotalBytesSent : 0) > 0);
		}

		[Fact]
		public void should_return_the_total_number_of_bytes_received_from_external_connections() {
			Assert.True(_results.Sum(r => r.IsExternalConnection ? r.TotalBytesReceived : 0) > 0);
		}

		[Fact]
		public void should_have_set_the_client_connection_name() {
			Assert.Contains(_results, x => x.ClientConnectionName == _clientConnectionName);
		}

		public override Task TestFixtureTearDown() {
			_portableServer.TearDown();
			_connection.Dispose();
			PortsHelper.ReturnPort(_serverEndPoint.Port);
			return base.TestFixtureTearDown();
		}
	}
}
