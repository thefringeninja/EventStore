using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class event_store_connection_should : SpecificationWithDirectoryPerTestFixture {
		private MiniNode _node;
		
		public static IEnumerable<object[]> TestCases() => new [] {new object[]{TcpType.Normal}, new object[]{TcpType.Ssl}}; 

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName);
			await _node.Start();
		}

		public override async Task TestFixtureTearDown() {
			await _node.Shutdown();
			await base.TestFixtureTearDown();
		}

		[Theory, MemberData(nameof(TestCases))]
		[Trait("Category", "Network")]
		public void not_throw_on_close_if_connect_was_not_called(TcpType tcpType) {
			var connection = TestConnection.To(_node, tcpType);
			connection.Close();
		}

		[Theory, MemberData(nameof(TestCases))]
		[Trait("Category", "Network")]
		public async Task not_throw_on_close_if_called_multiple_times(TcpType tcpType) {
			var connection = TestConnection.To(_node, tcpType);
			await connection.ConnectAsync();
			connection.Close();
			connection.Close();
		}

/*
//TODO WEIRD TEST GFY
        [Fact]
        [Trait("Category", "Network")]
        public void throw_on_connect_called_more_than_once()
        {
            var connection = TestConnection.To(_node, _tcpType);
            connection.ConnectAsync().Wait();

            await Assert.ThrowsAsync<>(() => connection.ConnectAsync().Wait(),
                        Throws.Exception.InstanceOf<AggregateException>().With.InnerException.InstanceOf<InvalidOperationException>());
        }

        [Fact]
        [Trait("Category", "Network")]
        public void throw_on_connect_called_after_close()
        {
            var connection = TestConnection.To(_node, _tcpType);
            connection.ConnectAsync().Wait();
            connection.Close();

            await Assert.ThrowsAsync<>(() => connection.ConnectAsync().Wait(),
                        Throws.Exception.InstanceOf<AggregateException>().With.InnerException.InstanceOf<InvalidOperationException>());
        }
*/
		[Theory, MemberData(nameof(TestCases))]
		[Trait("Category", "Network")]
		public async Task throw_invalid_operation_on_every_api_call_if_connect_was_not_called(TcpType tcpType) {
			var connection = TestConnection.To(_node, tcpType);

			const string s = "stream";
			var events = new[] {TestEvent.NewTestEvent()};

			await Assert.ThrowsAsync<InvalidOperationException>(() => connection.DeleteStreamAsync(s, 0));

			await Assert.ThrowsAsync<InvalidOperationException>(() => connection.AppendToStreamAsync(s, 0, events));

			await Assert.ThrowsAsync<InvalidOperationException>(
				() => connection.ReadStreamEventsForwardAsync(s, 0, 1, resolveLinkTos: false));

			await Assert.ThrowsAsync<InvalidOperationException>(
				() => connection.ReadStreamEventsBackwardAsync(s, 0, 1, resolveLinkTos: false));

			await Assert.ThrowsAsync<InvalidOperationException>(
				() => connection.ReadAllEventsForwardAsync(Position.Start, 1, false));

			await Assert.ThrowsAsync<InvalidOperationException>(() =>
				connection.ReadAllEventsBackwardAsync(Position.End, 1, false));

			await Assert.ThrowsAsync<InvalidOperationException>(() => connection.StartTransactionAsync(s, 0));

			await Assert.ThrowsAsync<InvalidOperationException>(
				() => connection.SubscribeToStreamAsync(s, false, (_, __) => Task.CompletedTask, (_, __, ___) => { }));

			await Assert.ThrowsAsync<InvalidOperationException>(
				() => connection.SubscribeToAllAsync(false, (_, __) => Task.CompletedTask, (_, __, ___) => { }));
		}
	}
}
