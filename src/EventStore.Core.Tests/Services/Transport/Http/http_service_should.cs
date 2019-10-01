using System;
using System.Net;
using System.Threading;
using EventStore.Core.Messages;
using EventStore.Core.Tests.Helpers;
using EventStore.Transport.Http;
using Xunit;
using EventStore.Common.Utils;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Http;
using System.Linq;
using HttpStatusCode = System.Net.HttpStatusCode;

namespace EventStore.Core.Tests.Services.Transport.Http {
	[Trait("Category", "LongRunning")]
	public class http_service_should : IDisposable {
		private readonly IPEndPoint _serverEndPoint;
		private readonly PortableServer _portableServer;

		public http_service_should() {
			var port = PortsHelper.GetAvailablePort(IPAddress.Loopback);
			_serverEndPoint = new IPEndPoint(IPAddress.Loopback, port);
			_portableServer = new PortableServer(_serverEndPoint);
			_portableServer.SetUp();
		}

		public void Dispose() {
			_portableServer.TearDown();
			PortsHelper.ReturnPort(_serverEndPoint.Port);
		}

		[Fact]
		[Trait("Category", "Network")]
		public void start_after_system_message_system_init_published() {
			Assert.False(_portableServer.IsListening);
			_portableServer.Publish(new SystemMessage.SystemInit());
			Assert.True(_portableServer.IsListening);
		}

		[Fact]
		[Trait("Category", "Network")]
		public void ignore_shutdown_message_that_does_not_say_shut_down() {
			_portableServer.Publish(new SystemMessage.SystemInit());
			Assert.True(_portableServer.IsListening);

			_portableServer.Publish(
				new SystemMessage.BecomeShuttingDown(Guid.NewGuid(), exitProcess: false, shutdownHttp: false));
			Assert.True(_portableServer.IsListening);
		}

		[Fact]
		[Trait("Category", "Network")]
		public void react_to_shutdown_message_that_cause_process_exit() {
			_portableServer.Publish(new SystemMessage.SystemInit());
			Assert.True(_portableServer.IsListening);

			_portableServer.Publish(
				new SystemMessage.BecomeShuttingDown(Guid.NewGuid(), exitProcess: true, shutdownHttp: true));
			Assert.False(_portableServer.IsListening);
		}

		[Fact]
		[Trait("Category", "Network")]
		public void reply_with_404_to_every_request_when_there_are_no_registered_controllers() {
			var requests = new[] {"/ping", "/streams", "/gossip", "/stuff", "/notfound", "/magic/url.exe"};
			var successes = new bool[requests.Length];
			var errors = new string[requests.Length];
			var signals = new AutoResetEvent[requests.Length];
			for (var i = 0; i < signals.Length; i++)
				signals[i] = new AutoResetEvent(false);

			_portableServer.Publish(new SystemMessage.SystemInit());

			for (var i = 0; i < requests.Length; i++) {
				var i1 = i;
				_portableServer.BuiltInClient.Get(
					_serverEndPoint.ToHttpUrl(EndpointExtensions.HTTP_SCHEMA, requests[i]),
					response => {
						successes[i1] = response.HttpStatusCode == (int)HttpStatusCode.NotFound;
						signals[i1].Set();
					},
					exception => {
						successes[i1] = false;
						errors[i1] = exception.Message;
						signals[i1].Set();
					});
			}

			foreach (var signal in signals)
				signal.WaitOne();

			Assert.True(successes.All(x => x), string.Join(";", errors.Where(e => !string.IsNullOrEmpty(e))));
		}

		[Fact]
		[Trait("Category", "Network")]
		public void handle_invalid_characters_in_url() {
			var url = _serverEndPoint.ToHttpUrl(EndpointExtensions.HTTP_SCHEMA, "/ping^\"");
			Func<HttpResponse, bool> verifier = response => string.IsNullOrEmpty(response.Body) &&
			                                                response.HttpStatusCode == (int)HttpStatusCode.NotFound;

			var result = _portableServer.StartServiceAndSendRequest(HttpBootstrap.RegisterPing, url, verifier);
			Assert.True(result.Item1, result.Item2);
		}
	}


	[Trait("Category", "LongRunning")]
	public class when_http_request_times_out : IDisposable {
		private readonly IPEndPoint _serverEndPoint;
		private readonly PortableServer _portableServer;
		private int _timeout;

		public when_http_request_times_out() {
			_timeout = 2000;
			var port = PortsHelper.GetAvailablePort(IPAddress.Loopback);
			_serverEndPoint = new IPEndPoint(IPAddress.Loopback, port);
			_portableServer = new PortableServer(_serverEndPoint, _timeout);
			_portableServer.SetUp();
		}

		public void Dispose() {
			_portableServer.TearDown();
			PortsHelper.ReturnPort(_serverEndPoint.Port);
		}

		[Fact]
		[Trait("Category", "Network")]
		public void should_throw_an_exception() {
			var sleepFor = _timeout + 1000;
			var url = _serverEndPoint.ToHttpUrl(EndpointExtensions.HTTP_SCHEMA,
				string.Format("/test-timeout?sleepfor={0}", sleepFor));
			Func<HttpResponse, bool> verifier = response => { return true; };
			var result = _portableServer.StartServiceAndSendRequest(service =>
				service.SetupController(new TestController(new FakePublisher())), url, verifier);
			Assert.False(result.Item1, "Should not have got a response"); // We should not have got a response
			Assert.True(!string.IsNullOrEmpty(result.Item2), "Error was empty");
		}
	}
}
