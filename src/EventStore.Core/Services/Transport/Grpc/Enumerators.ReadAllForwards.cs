using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Claims;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Client;
using EventStore.Client.Streams;
using Grpc.Core;

namespace EventStore.Core.Services.Transport.Grpc {
	internal static partial class Enumerators {
		public class ReadAllForwards : IAsyncEnumerator<ReadResp> {
			private readonly IPublisher _bus;
			private readonly ulong _maxCount;
			private readonly bool _resolveLinks;
			private readonly ClaimsPrincipal _user;
			private readonly bool _requiresLeader;
			private readonly DateTime _deadline;
			private readonly ReadReq.Types.Options.Types.UUIDOption _uuidOption;
			private readonly CancellationToken _cancellationToken;
			private readonly SemaphoreSlim _semaphore;
			private readonly Channel<ReadResp> _channel;

			private ReadResp _current;
			private ulong _readCount;

			public ReadResp Current => _current;

			public ReadAllForwards(IPublisher bus,
				Position position,
				ulong maxCount,
				bool resolveLinks,
				ClaimsPrincipal user,
				bool requiresLeader,
				DateTime deadline,
				ReadReq.Types.Options.Types.UUIDOption uuidOption,
				CancellationToken cancellationToken) {
				if (bus == null) {
					throw new ArgumentNullException(nameof(bus));
				}

				_bus = bus;
				_maxCount = maxCount;
				_resolveLinks = resolveLinks;
				_user = user;
				_requiresLeader = requiresLeader;
				_deadline = deadline;
				_uuidOption = uuidOption;
				_cancellationToken = cancellationToken;
				_semaphore = new SemaphoreSlim(1, 1);
				_channel = Channel.CreateBounded<ReadResp>(BoundedChannelOptions);

				ReadPage(position);
			}

			public ValueTask DisposeAsync() {
				_channel.Writer.TryComplete();
				return new ValueTask(Task.CompletedTask);
			}

			public async ValueTask<bool> MoveNextAsync() {
				if (_readCount >= _maxCount) {
					return false;
				}

				if (!await _channel.Reader.WaitToReadAsync(_cancellationToken).ConfigureAwait(false)) {
					return false;
				}

				_current = await _channel.Reader.ReadAsync(_cancellationToken).ConfigureAwait(false);
				_readCount++;
				return true;
			}

			private void ReadPage(Position startPosition) {
				var correlationId = Guid.NewGuid();

				var (commitPosition, preparePosition) = startPosition.ToInt64();

				_bus.Publish(new ClientMessage.ReadAllEventsForward(
					correlationId, correlationId, new ContinuationEnvelope(OnMessage, _semaphore, _cancellationToken),
					commitPosition, preparePosition, (int)Math.Min(ReadBatchSize, _maxCount), _resolveLinks,
					_requiresLeader, default, _user, expires: _deadline));

				async Task OnMessage(Message message, CancellationToken ct) {
					if (message is ClientMessage.NotHandled notHandled &&
					    RpcExceptions.TryHandleNotHandled(notHandled, out var ex)) {
						_channel.Writer.TryComplete(ex);
						return;
					}

					if (!(message is ClientMessage.ReadAllEventsForwardCompleted completed)) {
						_channel.Writer.TryComplete(
							RpcExceptions.UnknownMessage<ClientMessage.ReadAllEventsForwardCompleted>(message));
						return;
					}

					switch (completed.Result) {
						case ReadAllResult.Success:
							foreach (var @event in completed.Events) {
								await _channel.Writer.WriteAsync(new ReadResp {
									Event = ConvertToReadEvent(_uuidOption, @event)
								}, _cancellationToken).ConfigureAwait(false);
							}

							if (completed.IsEndOfStream) {
								_channel.Writer.TryComplete();
								return;
							}

							ReadPage(Position.FromInt64(
								completed.NextPos.CommitPosition,
								completed.NextPos.PreparePosition));
							return;
						case ReadAllResult.AccessDenied:
							_channel.Writer.TryComplete(RpcExceptions.AccessDenied());
							return;
						default:
							_channel.Writer.TryComplete(RpcExceptions.UnknownError(completed.Result));
							return;
					}
				}
			}
		}
	}
}
