using System.Threading.Channels;
using EventStore.Client.Shared;
using EventStore.Client.Streams;
using EventStore.Core.Data;
using Google.Protobuf;

namespace EventStore.Core.Services.Transport.Grpc {
	internal static partial class Enumerators {
		private const int MaxLiveEventBufferCount = 16;
		private const int ReadBatchSize = 32; // TODO  JPB make this configurable

		private static readonly BoundedChannelOptions BoundedChannelOptions =
			new BoundedChannelOptions(MaxLiveEventBufferCount) {
				FullMode = BoundedChannelFullMode.Wait,
				SingleReader = true,
				SingleWriter = true
			};

		private static ReadResp.Types.ReadEvent.Types.RecordedEvent ConvertToRecordedEvent(
			ReadReq.Types.Options.Types.UUIDOption uuidOption, EventRecord e, long? commitPosition,
			long? preparePosition) {
			if (e == null) return null;
			var position = Position.FromInt64(commitPosition ?? -1, preparePosition ?? -1);
			return new ReadResp.Types.ReadEvent.Types.RecordedEvent {
				Id = uuidOption.ContentCase switch {
					ReadReq.Types.Options.Types.UUIDOption.ContentOneofCase.String => new UUID {
						String = e.EventId.ToString()
					},
					_ => Uuid.FromGuid(e.EventId).ToDto()
				},
				StreamIdentifier = e.EventStreamId,
				StreamRevision = StreamRevision.FromInt64(e.EventNumber),
				CommitPosition = position.CommitPosition,
				PreparePosition = position.PreparePosition,
				Metadata = {
					[Constants.Metadata.Type] = e.EventType,
					[Constants.Metadata.Created] = e.TimeStamp.ToTicksSinceEpoch().ToString(),
					[Constants.Metadata.ContentType] = e.IsJson
						? Constants.Metadata.ContentTypes.ApplicationJson
						: Constants.Metadata.ContentTypes.ApplicationOctetStream
				},
				Data = ByteString.CopyFrom(e.Data.Span),
				CustomMetadata = ByteString.CopyFrom(e.Metadata.Span)
			};
		}

		private static ReadResp.Types.ReadEvent ConvertToReadEvent(ReadReq.Types.Options.Types.UUIDOption uuidOption,
			ResolvedEvent e, long lastEventNumber, long completedTfLastCommitPosition) {
			var lastPosition = Position.FromInt64(completedTfLastCommitPosition, completedTfLastCommitPosition);
			var readEvent = new ReadResp.Types.ReadEvent {
				Link = ConvertToRecordedEvent(uuidOption, e.Link, e.OriginalPosition?.CommitPosition,
					e.OriginalPosition?.PreparePosition),
				Event = ConvertToRecordedEvent(uuidOption, e.Event, e.OriginalPosition?.CommitPosition,
					e.OriginalPosition?.PreparePosition),
				ServerPosition = new ReadResp.Types.ReadEvent.Types.ServerPosition {
					LastStreamPosition = StreamRevision.FromInt64(lastEventNumber),
					LastCommitPosition = lastPosition.CommitPosition,
					LastPreparePosition = lastPosition.PreparePosition
				}
			};
			if (e.OriginalPosition.HasValue) {
				var position = Position.FromInt64(
					e.OriginalPosition.Value.CommitPosition,
					e.OriginalPosition.Value.PreparePosition);
				readEvent.CommitPosition = position.CommitPosition;
			} else {
				readEvent.NoPosition = new Empty();
			}

			return readEvent;
		}
	}
}
