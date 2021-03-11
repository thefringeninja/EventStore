using System.Linq;
using EventStore.Client.Shared;
using EventStore.Client.Streams;
using Google.Protobuf;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Transport.Grpc.StreamsTests {
	[TestFixture]
	public class ReadStreamsBackwardTests {
		public class when_reading_backward_from_past_the_end_of_the_stream : GrpcSpecification.Read {
			protected override int EventCount => 30;

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
					Count = 20,
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Backwards,
					ResolveLinks = false,
					Stream = new ReadReq.Types.Options.Types.StreamOptions {
						StreamIdentifier = new StreamIdentifier {
							StreamName = ByteString.CopyFromUtf8(StreamName)
						},
						Revision = 50
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
				Assert.AreEqual(29, ReadResponses[0].Event.Event.StreamRevision);
				Assert.AreEqual(10, ReadResponses[^1].Event.Event.StreamRevision);
			}
		}

		public class when_reading_backward_from_the_end_of_the_stream : GrpcSpecification.Read {
			protected override int EventCount => 30;

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
					Count = 20,
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Backwards,
					ResolveLinks = false,
					Stream = new ReadReq.Types.Options.Types.StreamOptions {
						StreamIdentifier = new StreamIdentifier {
							StreamName = ByteString.CopyFromUtf8(StreamName)
						},
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
				Assert.AreEqual(29, ReadResponses[0].Event.Event.StreamRevision);
				Assert.AreEqual(10, ReadResponses[^1].Event.Event.StreamRevision);
			}
		}

		public class when_reading_backward_from_the_start_of_the_stream : GrpcSpecification.Read {
			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
					Count = 20,
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Backwards,
					ResolveLinks = false,
					Stream = new ReadReq.Types.Options.Types.StreamOptions {
						StreamIdentifier = new StreamIdentifier {
							StreamName = ByteString.CopyFromUtf8(StreamName)
						},
						Start = new Empty()
					},
					NoFilter = new Empty()
				}
			};

			[Test]
			public void should_receive_the_first_event() {
				Assert.AreEqual(1, ReadResponses.Count);
				Assert.AreEqual(0, ReadResponses[0].Event.OriginalEvent.StreamRevision);
			}
		}
	}
}
