using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.Client.Shared;
using EventStore.Client.Streams;
using EventStore.ClientAPI;
using EventStore.Core.Services;
using Google.Protobuf;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Transport.Grpc.StreamsTests {
	[TestFixture]
	public class ReadStreamsForwardTests {
		public class when_reading_forward_from_stream_that_has_been_truncated : GrpcSpecification.Read {
			protected override int EventCount => 100;

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
					Count = 10,
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Forwards,
					ResolveLinks = false,
					Stream = new ReadReq.Types.Options.Types.StreamOptions {
						StreamIdentifier = new StreamIdentifier {
							StreamName = ByteString.CopyFromUtf8(StreamName)
						},
						Revision = 0
					},
					NoFilter = new Empty()
				}
			};

			protected override async Task Given() {
				await base.Given();

				await AppendToStream(SystemStreams.MetastreamOf(StreamName), new[] {
					new EventData(Guid.NewGuid(), SystemEventTypes.StreamMetadata, true,
						Encoding.UTF8.GetBytes($@"{{ ""{SystemMetadata.TruncateBefore}"": 81 }}"), Array.Empty<byte>())
				});
			}

			[Test]
			public void should_not_receive_null_events() {
				Assert.False(ReadResponses.Any(x => x.Event is null));
			}

			[Test]
			public void should_read_a_number_of_events_equal_to_the_max_count() {
				Assert.AreEqual(10, ReadResponses.Count);
			}

			[Test]
			public void should_start_from_the_truncation_position() {
				Assert.AreEqual(81, ReadResponses[0].Event.OriginalEvent.StreamRevision);
				Assert.AreEqual(90, ReadResponses[^1].Event.OriginalEvent.StreamRevision);
			}

			[Test]
			public void should_indicate_last_position_of_stream() {
				CollectionAssert.AreEqual(Enumerable.Range(0, EventCount)
					.Skip(81)
					.Take(10)
					.Select(streamPosition => new {
						streamPosition = Convert.ToUInt64(streamPosition),
						lastStreamPosition = EventCount - 1
					}), ReadResponses.Where(x => x.ContentCase == ReadResp.ContentOneofCase.Event)
					.Select(x => new {
						streamPosition = x.Event.OriginalEvent.StreamRevision,
						lastStreamPosition = EventCount - 1
					}));
			}
		}

		public class when_reading_forward_from_the_start_of_the_stream : GrpcSpecification.Read {
			protected override int EventCount => 100;

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
					Count = 50,
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Forwards,
					ResolveLinks = false,
					Stream = new ReadReq.Types.Options.Types.StreamOptions {
						StreamIdentifier = new StreamIdentifier {
							StreamName = ByteString.CopyFromUtf8(StreamName)
						},
						Revision = 0
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
				Assert.AreEqual(50, ReadResponses.Count);
			}

			[Test]
			public void should_read_the_correct_events() {
				Assert.AreEqual(49, ReadResponses[^1].Event.OriginalEvent.StreamRevision);
			}

			[Test]
			public void should_indicate_last_position_of_stream() {
				CollectionAssert.AreEqual(Enumerable.Range(0, EventCount)
					.Take(50)
					.Select(streamPosition => new {
						streamPosition = Convert.ToUInt64(streamPosition),
						lastStreamPosition = EventCount - 1
					}), ReadResponses.Where(x => x.ContentCase == ReadResp.ContentOneofCase.Event)
					.Select(x => new {
						streamPosition = x.Event.OriginalEvent.StreamRevision,
						lastStreamPosition = EventCount - 1
					}));
			}

		}

		public class when_reading_forward_from_stream_with_no_events_after_position : GrpcSpecification.Read {
			protected override int EventCount => 10;

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
					Count = 50,
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Forwards,
					ResolveLinks = false,
					Stream = new ReadReq.Types.Options.Types.StreamOptions {
						StreamIdentifier = new StreamIdentifier {
							StreamName = ByteString.CopyFromUtf8(StreamName)
						},
						Revision = 11
					},
					NoFilter = new Empty()
				}
			};

			[Test]
			public void should_not_receive_events() {
				Assert.IsEmpty(ReadResponses);
			}
		}
	}
}
