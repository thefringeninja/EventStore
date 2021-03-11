using System;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Client.Shared;
using EventStore.Client.Streams;
using EventStore.ClientAPI;
using NUnit.Framework;
using Position = EventStore.Core.Services.Transport.Grpc.Position;

namespace EventStore.Core.Tests.Services.Transport.Grpc.StreamsTests {
	[TestFixture]
	public class ReadAllBackwardsTests {
		public class when_reading_all_backwards : GrpcSpecification.Read {
			protected override int EventCount => 50;

			protected override (string userName, string password) DefaultCredentials => AdminCredentials;

			protected override async Task Given() {
				await base.Given();

				await AppendToStream(nameof(when_reading_all_backwards),
					Enumerable.Range(0, 10).Select(_ =>
						new EventData(Guid.NewGuid(), "-", false, Array.Empty<byte>(), Array.Empty<byte>())));
			}

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
					Count = 20,
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Backwards,
					ResolveLinks = false,
					All = new ReadReq.Types.Options.Types.AllOptions {
						Position = new ReadReq.Types.Options.Types.Position {
							CommitPosition = PositionOfLastWrite.CommitPosition,
							PreparePosition = PositionOfLastWrite.CommitPosition
						}
					},
					NoFilter = new Empty()
				}
			};

			[Test]
			public void should_not_receive_null_events() {
				Assert.False(ReadResponses.Any(x => x.Event is null));
			}

			[Test]
			public void should_read_a_number_of_events_equal_to_the_max_count() {
				Assert.AreEqual(20, ReadResponses.Count);
			}

			[Test]
			public void should_read_the_correct_events() {
				Assert.True(ReadResponses
					.Where(x => x.ContentCase == ReadResp.ContentOneofCase.Event)
					.All(x =>
						new Position(x.Event.OriginalEvent.CommitPosition, x.Event.OriginalEvent.PreparePosition) <=
						PositionOfLastWrite));
			}
		}

		public class when_reading_all_backwards_from_end : GrpcSpecification.Read {
			protected override int EventCount => 50;

			protected override (string userName, string password) DefaultCredentials => AdminCredentials;

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
					Count = 20,
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Backwards,
					ResolveLinks = false,
					All = new ReadReq.Types.Options.Types.AllOptions {
						End = new Empty()
					},
					NoFilter = new Empty()
				}
			};

			[Test]
			public void should_not_receive_null_events() {
				Assert.False(ReadResponses.Any(x => x.Event is null));
			}

			[Test]
			public void should_read_a_number_of_events_equal_to_the_max_count() {
				Assert.AreEqual(20, ReadResponses.Count);
			}

			[Test]
			public void should_read_the_correct_events() {
				Assert.AreEqual(49, ReadResponses[0].Event.Event.StreamRevision);
				Assert.AreEqual(30, ReadResponses[^1].Event.Event.StreamRevision);
			}
		}
	}
}
