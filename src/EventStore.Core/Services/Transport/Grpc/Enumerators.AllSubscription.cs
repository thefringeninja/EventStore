using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Client;

namespace EventStore.Core.Services.Transport.Grpc {
	partial class Enumerators {
		public class AllSubscription : IAsyncEnumerator<ResolvedEvent> {
			private static readonly ILogger Log = LogManager.GetLoggerFor<StreamSubscription>();

			private readonly Guid _subscriptionId;
			private readonly IPublisher _bus;
			private readonly Position? _startPosition;
			private readonly bool _resolveLinks;
			private readonly IPrincipal _user;
			private readonly IReadIndex _readIndex;
			private readonly CancellationToken _cancellationToken;
			private IAsyncEnumerator<ResolvedEvent> _inner;

			private bool _catchUpRestarted;

			public ResolvedEvent Current => _inner.Current;

			private Position CurrentPosition =>
				_inner.Current.OriginalPosition.HasValue
					? Position.FromInt64(_inner.Current.OriginalPosition.Value.CommitPosition,
						_inner.Current.OriginalPosition.Value.PreparePosition)
					: Position.Start;

			public AllSubscription(IPublisher bus,
				Position? startPosition,
				bool resolveLinks,
				IPrincipal user,
				IReadIndex readIndex,
				CancellationToken cancellationToken) {
				if (bus == null) {
					throw new ArgumentNullException(nameof(bus));
				}

				if (readIndex == null) {
					throw new ArgumentNullException(nameof(readIndex));
				}

				_subscriptionId = Guid.NewGuid();
				_bus = bus;
				_startPosition = startPosition == Position.End ? Position.Start : startPosition;
				_resolveLinks = resolveLinks;
				_user = user;
				_readIndex = readIndex;
				_cancellationToken = cancellationToken;

				_inner = new CatchupAllSubscription(_subscriptionId, bus, startPosition ?? Position.Start, resolveLinks,
					user, readIndex, cancellationToken);
			}

			public async ValueTask<bool> MoveNextAsync() {
				if (!await MoveNextCoreAsync().ConfigureAwait(false)) {
					return false;
				}

				if (!_startPosition.HasValue) {
					return true;
				}

				if (_startPosition >= CurrentPosition) {
					return await MoveNextCoreAsync().ConfigureAwait(false);
				}

				return true;
			}

			private async Task<bool> MoveNextCoreAsync() {
				if (await _inner.MoveNextAsync().ConfigureAwait(false)) {
					return true;
				}

				if (!_catchUpRestarted) {
					await _inner.DisposeAsync().ConfigureAwait(false);

					_inner = new LiveStreamSubscription(_subscriptionId, _bus, OnLiveSubscriptionDropped, _resolveLinks, _user, _cancellationToken);
				} else {
					_catchUpRestarted = true;
				}

				return await _inner.MoveNextAsync().ConfigureAwait(false);
			}

			public ValueTask DisposeAsync() => _inner.DisposeAsync();

			private async ValueTask OnLiveSubscriptionDropped(Position caughtUpPosition) {
				Log.Trace(
					"Live subscription {subscriptionId} to $all dropped, reading will resume after {caughtUpPosition}",
					_subscriptionId, caughtUpPosition);

				await _inner.DisposeAsync().ConfigureAwait(false);

				_inner = new CatchupAllSubscription(_subscriptionId, _bus, caughtUpPosition, _resolveLinks, _user,
					_readIndex, _cancellationToken);
			}

			private class CatchupAllSubscription : IAsyncEnumerator<ResolvedEvent> {
				private readonly Guid _subscriptionId;
				private readonly IPublisher _bus;
				private readonly bool _resolveLinks;
				private readonly IPrincipal _user;
				private readonly CancellationTokenSource _disposedTokenSource;
				private readonly ConcurrentQueue<ResolvedEvent> _buffer;
				private readonly CancellationTokenRegistration _tokenRegistration;
				private Position _nextPosition;
				private ResolvedEvent _current;

				public ResolvedEvent Current => _current;

				public CatchupAllSubscription(Guid subscriptionId,
					IPublisher bus,
					Position position,
					bool resolveLinks,
					IPrincipal user,
					IReadIndex readIndex,
					CancellationToken cancellationToken) {
					if (bus == null) {
						throw new ArgumentNullException(nameof(bus));
					}

					if (readIndex == null) {
						throw new ArgumentNullException(nameof(readIndex));
					}

					_subscriptionId = subscriptionId;
					_bus = bus;
					_nextPosition = position == Position.End
						? Position.FromInt64(readIndex.LastIndexedPosition, readIndex.LastIndexedPosition)
						: position;
					_resolveLinks = resolveLinks;
					_user = user;
					_disposedTokenSource = new CancellationTokenSource();
					_buffer = new ConcurrentQueue<ResolvedEvent>();
					_tokenRegistration = cancellationToken.Register(_disposedTokenSource.Dispose);
					Log.Info("Catch-up subscription {subscriptionId} to $all running...",
						_subscriptionId);
				}

				public ValueTask DisposeAsync() {
					_disposedTokenSource.Dispose();
					_tokenRegistration.Dispose();
					return default;
				}

				public async ValueTask<bool> MoveNextAsync() {
					ReadLoop:
					if (_disposedTokenSource.IsCancellationRequested) {
						return false;
					}

					if (_buffer.TryDequeue(out var current)) {
						_current = current;
						return true;
					}

					var correlationId = Guid.NewGuid();

					var readNextSource = new TaskCompletionSource<bool>();

					var (commitPosition, preparePosition) = _nextPosition.ToInt64();

					Log.Trace(
						"Catch-up subscription {subscriptionId} to $all: reading next page starting from {nextPosition}.",
						_subscriptionId, _nextPosition);

					_bus.Publish(new ClientMessage.ReadAllEventsForward(
						correlationId, correlationId, new CallbackEnvelope(OnMessage), commitPosition, preparePosition,
						32, _resolveLinks, false, default, _user));

					var isEnd = await readNextSource.Task.ConfigureAwait(false);

					if (_buffer.TryDequeue(out current)) {
						_current = current;
						return true;
					}

					if (isEnd) {
						return false;
					}

					if (_disposedTokenSource.IsCancellationRequested) {
						return false;
					}

					goto ReadLoop;

					void OnMessage(Message message) {
						if (message is ClientMessage.NotHandled notHandled &&
						    RpcExceptions.TryHandleNotHandled(notHandled, out var ex)) {
							readNextSource.TrySetException(ex);
							return;
						}

						if (!(message is ClientMessage.ReadAllEventsForwardCompleted completed)) {
							readNextSource.TrySetException(
								RpcExceptions.UnknownMessage<ClientMessage.ReadAllEventsForwardCompleted>(message));
							return;
						}

						switch (completed.Result) {
							case ReadAllResult.Success:
								foreach (var @event in completed.Events) {
									_buffer.Enqueue(@event);
								}

								_nextPosition = Position.FromInt64(
									completed.NextPos.CommitPosition,
									completed.NextPos.PreparePosition);
								readNextSource.TrySetResult(completed.IsEndOfStream);
								return;
							case ReadAllResult.AccessDenied:
								readNextSource.TrySetException(RpcExceptions.AccessDenied());
								return;
							default:
								readNextSource.TrySetException(RpcExceptions.UnknownError(completed.Result));
								return;
						}
					}
				}
			}

			private class LiveStreamSubscription : IAsyncEnumerator<ResolvedEvent> {
				private readonly ConcurrentQueueWrapper<(ResolvedEvent resolvedEvent, Exception exception)>
					_liveEventBuffer;

				private readonly Stack<(ResolvedEvent resolvedEvent, Exception exception)> _historicalEventBuffer;
				private readonly Guid _subscriptionId;
				private readonly IPublisher _bus;
				private readonly Func<Position, ValueTask> _onDropped;
				private readonly IPrincipal _user;
				private readonly TaskCompletionSource<Position> _subscriptionConfirmed;
				private readonly TaskCompletionSource<bool> _readHistoricalEventsCompleted;
				private readonly CancellationTokenRegistration _tokenRegistration;
				private readonly CancellationTokenSource _disposedTokenSource;

				private ResolvedEvent _current;

				public ResolvedEvent Current => _current;

				public LiveStreamSubscription(Guid subscriptionId,
					IPublisher bus,
					Func<Position, ValueTask> onDropped,
					bool resolveLinks,
					IPrincipal user,
					CancellationToken cancellationToken) {
					if (bus == null) {
						throw new ArgumentNullException(nameof(bus));
					}

					if (onDropped == null) {
						throw new ArgumentNullException(nameof(onDropped));
					}

					_liveEventBuffer = new ConcurrentQueueWrapper<(ResolvedEvent resolvedEvent, Exception exception)>();
					_historicalEventBuffer = new Stack<(ResolvedEvent resolvedEvent, Exception exception)>();
					_subscriptionId = subscriptionId;
					_bus = bus;
					_onDropped = onDropped;
					_user = user;
					_subscriptionConfirmed = new TaskCompletionSource<Position>();
					_readHistoricalEventsCompleted = new TaskCompletionSource<bool>();
					_disposedTokenSource = new CancellationTokenSource();
					_tokenRegistration = cancellationToken.Register(_disposedTokenSource.Dispose);

					Log.Info("Live subscription {subscriptionId} to $all running...", subscriptionId);

					bus.Publish(new ClientMessage.SubscribeToStream(Guid.NewGuid(), _subscriptionId,
						new CallbackEnvelope(OnSubscriptionMessage), subscriptionId, string.Empty, resolveLinks, user));

					void OnSubscriptionMessage(Message message) {
						if (message is ClientMessage.NotHandled notHandled &&
						    RpcExceptions.TryHandleNotHandled(notHandled, out var ex)) {
							_subscriptionConfirmed.TrySetException(ex);
							return;
						}

						switch (message) {
							case ClientMessage.SubscriptionConfirmation confirmed:
								var position = Position.FromInt64(confirmed.LastIndexedPosition,
									confirmed.LastIndexedPosition);
								Log.Trace(
									"Live subscription {subscriptionId} to $all confirmed at {position}",
									_subscriptionId, position);
								_subscriptionConfirmed.TrySetResult(position);
								ReadHistoricalEvents(position);
								return;
							case ClientMessage.SubscriptionDropped dropped:
								switch (dropped.Reason) {
									case SubscriptionDropReason.AccessDenied:
										Fail(RpcExceptions.AccessDenied());
										return;
									default:
										Fail(RpcExceptions.UnknownError(dropped.Reason));
										return;
								}
							case ClientMessage.StreamEventAppeared appeared:
								_liveEventBuffer.Enqueue((appeared.Event, null));
								return;
							default:
								Fail(RpcExceptions.UnknownMessage<ClientMessage.SubscriptionConfirmation>(message));
								return;
						}

						void Fail(Exception ex) {
							_liveEventBuffer.Enqueue((default, ex));
							_subscriptionConfirmed.TrySetException(ex);
						}
					}

					void OnHistoricalEventsMessage(Message message) {
						if (message is ClientMessage.NotHandled notHandled &&
						    RpcExceptions.TryHandleNotHandled(notHandled, out var ex)) {
							_readHistoricalEventsCompleted.TrySetException(ex);
							return;
						}

						if (!(message is ClientMessage.ReadAllEventsBackwardCompleted completed)) {
							_readHistoricalEventsCompleted.TrySetException(
								RpcExceptions.UnknownMessage<ClientMessage.ReadAllEventsBackwardCompleted>(message));
							return;
						}

						switch (completed.Result) {
							case ReadAllResult.Success:
								if (completed.Events.Length == 0) {
									Log.Trace("Live subscription {subscriptionId} to $all caught up.", _subscriptionId);
									_readHistoricalEventsCompleted.TrySetResult(true);
									return;
								}

								foreach (var @event in completed.Events) {
									var position = Position.FromInt64(@event.OriginalPosition.Value.CommitPosition,
										@event.OriginalPosition.Value.PreparePosition);
									if (position < _subscriptionConfirmed.Task.Result) {
										Log.Trace("Live subscription {subscriptionId} to $all caught up at {position}.",
											_subscriptionId, _subscriptionConfirmed.Task.Result);
										_readHistoricalEventsCompleted.TrySetResult(true);
										return;
									}

									_historicalEventBuffer.Push((@event, null));
								}

								ReadHistoricalEvents(Position.FromInt64(
									completed.NextPos.CommitPosition,
									completed.NextPos.PreparePosition));
								return;
							case ReadAllResult.AccessDenied:
								_readHistoricalEventsCompleted.TrySetException(RpcExceptions.AccessDenied());
								return;
							default:
								_readHistoricalEventsCompleted.TrySetException(
									RpcExceptions.UnknownError(completed.Result));
								return;
						}
					}

					void ReadHistoricalEvents(Position fromPosition) {
						Log.Trace(
							"Live subscription {subscriptionId} to $all loading any missed events starting at {fromPosition}",
							subscriptionId, fromPosition);

						var correlationId = Guid.NewGuid();
						var (commitPosition, preparePosition) = fromPosition.ToInt64();
						bus.Publish(new ClientMessage.ReadAllEventsBackward(correlationId, correlationId,
							new CallbackEnvelope(OnHistoricalEventsMessage), commitPosition, preparePosition, 32,
							resolveLinks, false, null, user));
					}
				}

				public async ValueTask<bool> MoveNextAsync() {
					(ResolvedEvent, Exception) _;

					await Task.WhenAll(_subscriptionConfirmed.Task, _readHistoricalEventsCompleted.Task)
						.ConfigureAwait(false);

					var livePosition = _subscriptionConfirmed.Task.Result;

					if (_historicalEventBuffer.TryPop(out _)) {
						var (historicalEvent, historicalException) = _;

						if (historicalException != null) {
							throw historicalException;
						}

						var position = Position.FromInt64(
							historicalEvent.OriginalPosition.Value.CommitPosition,
							historicalEvent.OriginalPosition.Value.PreparePosition);

						if (_liveEventBuffer.Count > MaxLiveEventBufferCount) {
							Log.Warn("Live subscription {subscriptionId} to $all buffer is full.",
								_subscriptionId);

							_liveEventBuffer.Clear();
							_historicalEventBuffer.Clear();

							await _onDropped(position).ConfigureAwait(false);
							return false;
						}

						if (livePosition > position) {
							_current = historicalEvent;
							Log.Trace(
								"Live subscription {subscriptionId} to $all received event {position} historically.",
								_subscriptionId, position);
							return true;
						}
					}

					var delay = 1;

					while (!_liveEventBuffer.TryDequeue(out _)) {
						await Task.Delay(Math.Max(delay *= 2, 50), _disposedTokenSource.Token)
							.ConfigureAwait(false);
					}

					var (resolvedEvent, exception) = _;

					if (exception != null) {
						throw exception;
					}

					Log.Trace("Live subscription {subscriptionId} to $all received event {position} live.",
						_subscriptionId, resolvedEvent.OriginalPosition);
					_current = resolvedEvent;
					return true;
				}

				public ValueTask DisposeAsync() {
					Log.Info("Live subscription {subscriptionId} to $all disposed.", _subscriptionId);
					_bus.Publish(new ClientMessage.UnsubscribeFromStream(Guid.NewGuid(), _subscriptionId,
						new NoopEnvelope(), _user));
					_disposedTokenSource.Dispose();
					_tokenRegistration.Dispose();
					return default;
				}
			}
		}
	}
}
