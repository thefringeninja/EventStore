using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Client.Shared;
using EventStore.Client.Streams;
using EventStore.Core.Services;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Transport.Grpc.StreamsTests {
	[TestFixture]
	public class SubscribeToAllTests {
		public class when_subscribing_to_all : GrpcSpecification.Read {
			protected override int EventCount => 120;
			protected override (string userName, string password) DefaultCredentials => AdminCredentials;

			protected override IAsyncEnumerator<ReadResp> GetEnumerator(IAsyncEnumerable<ReadResp> enumerable) =>
				new SubscriptionEnumerator(enumerable, PositionOfLastWrite);

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					All = new ReadReq.Types.Options.Types.AllOptions {
						Start = new Empty()
					},
					Subscription = new ReadReq.Types.Options.Types.SubscriptionOptions(),
					NoFilter = new Empty(),
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Forwards,
					ResolveLinks = false,
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
				}
			};

			[Test]
			public void subscription_confirmed() {
				Assert.AreEqual(ReadResp.ContentOneofCase.Confirmation, ReadResponses[0].ContentCase);
				Assert.NotNull(ReadResponses[0].Confirmation.SubscriptionId);
			}
		}

		public class when_subscribing_to_all_live : GrpcSpecification.Read {
			protected override int EventCount => 120;
			protected override (string userName, string password) DefaultCredentials => AdminCredentials;

			protected override IAsyncEnumerator<ReadResp> GetEnumerator(IAsyncEnumerable<ReadResp> enumerable) =>
				new SubscriptionEnumerator(enumerable, PositionOfLastWrite);

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					All = new ReadReq.Types.Options.Types.AllOptions {
						End = new Empty()
					},
					Subscription = new ReadReq.Types.Options.Types.SubscriptionOptions(),
					NoFilter = new Empty(),
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Forwards,
					ResolveLinks = false,
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
				}
			};

			protected override async Task When() {
				var when = base.When();

				while (ReadResponses.Count == 0) { // the task is hot, wait for subscription confirmation first.
					await Task.Delay(10);
				}

				await AppendToStream(StreamName, CreateEvents(1));

				try {
					await when.WithTimeout(TimeSpan.FromMilliseconds(2000));
				}
				catch(TimeoutException) {}
			}

			[Test]
			public void subscription_confirmed() {
				Assert.AreEqual(ReadResp.ContentOneofCase.Confirmation, ReadResponses[0].ContentCase);
				Assert.NotNull(ReadResponses[0].Confirmation.SubscriptionId);
			}

			[Test]
			public void reads_all_the_live_events() {
				Assert.AreEqual(1,
					ReadResponses.Count(x => x.ContentCase == ReadResp.ContentOneofCase.Event &&
					                         !x.Event.OriginalEvent.Metadata["type"].StartsWith("$")
					));
			}
		}
	}
}
