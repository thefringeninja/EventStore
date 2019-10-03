using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI"), Trait("Category", "LongRunning")]
	public class isjson_flag_on_event : IClassFixture<isjson_flag_on_event.Fixture> { public class Fixture : SpecificationWithDirectory {
		private MiniNode _node;

		public override async Task SetUp() {
			await base.SetUp();
			_node = new MiniNode(PathName);
			await _node.Start();
		}

		public override async Task TearDown() {
			await _node.Shutdown();
			await base.TearDown();
		}

		protected virtual IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.To(node, TcpType.Normal);
		}

		[Fact, Trait("Category", "LongRunning"), Trait("Category", "Network")]
		public async Task should_be_preserved_with_all_possible_write_and_read_methods() {
			const string stream = "should_be_preserved_with_all_possible_write_methods";
			using (var connection = BuildConnection(_node)) {
                await connection.ConnectAsync();

                await connection.AppendToStreamAsync(
						stream,
						ExpectedVersion.Any,
						new EventData(Guid.NewGuid(), "some-type", true,
							Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"), null),
						new EventData(Guid.NewGuid(), "some-type", true, null,
							Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}")),
						new EventData(Guid.NewGuid(), "some-type", true,
							Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"),
							Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}")));

				using (var transaction = await connection.StartTransactionAsync(stream, ExpectedVersion.Any)) {
                    await transaction.WriteAsync(
						new EventData(Guid.NewGuid(), "some-type", true,
							Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"), null),
						new EventData(Guid.NewGuid(), "some-type", true, null,
							Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}")),
						new EventData(Guid.NewGuid(), "some-type", true,
							Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}"),
							Helper.UTF8NoBom.GetBytes("{\"some\":\"json\"}")));
                    await transaction.CommitAsync();
				}

				var done = new ManualResetEventSlim();
				_node.Node.MainQueue.Publish(new ClientMessage.ReadStreamEventsForward(
					Guid.NewGuid(), Guid.NewGuid(), new CallbackEnvelope(message => {
						Assert.IsType<ClientMessage.ReadStreamEventsForwardCompleted>(message);
						var msg = (ClientMessage.ReadStreamEventsForwardCompleted)message;
						Assert.Equal(Data.ReadStreamResult.Success, msg.Result);
						Assert.Equal(6, msg.Events.Length);
						Assert.True(msg.Events.All(x => (x.OriginalEvent.Flags & PrepareFlags.IsJson) != 0));

						done.Set();
					}), stream, 0, 100, false, false, null, null));
				Assert.True(done.Wait(10000), "Read was not completed in time.");
			}
		}
	}
}
