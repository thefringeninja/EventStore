using System;
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
using EventStore.Client;
using IEventFilter = EventStore.Core.Util.IEventFilter;
using IReadIndex = EventStore.Core.Services.Storage.ReaderIndex.IReadIndex;

namespace EventStore.Core.Services.Transport.Grpc {
	internal static partial class Enumerators {
		public class AllSubscriptionFiltered : IAsyncEnumerator<ResolvedEvent> {
			private static readonly ILogger Log = LogManager.GetLoggerFor<StreamSubscription>();

			private readonly Guid _subscriptionId;
			private readonly IPublisher _bus;
			private readonly Position? _startPosition;
			private readonly bool _resolveLinks;
			private readonly IEventFilter _eventFilter;
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

			public AllSubscriptionFiltered(IPublisher bus,
				Position? startPosition,
				bool resolveLinks,
				IEventFilter eventFilter,
				IPrincipal user,
				IReadIndex readIndex,
				CancellationToken cancellationToken) {
				if (bus == null) {
					throw new ArgumentNullException(nameof(bus));
				}

				if (eventFilter == null) {
					throw new ArgumentNullException(nameof(eventFilter));
				}

				if (readIndex == null) {
					throw new ArgumentNullException(nameof(readIndex));
				}

				_subscriptionId = Guid.NewGuid();
				_bus = bus;
				_startPosition = startPosition == Position.End ? Position.Start : startPosition;
				_resolveLinks = resolveLinks;
				_eventFilter = eventFilter;
				_user = user;
				_readIndex = readIndex;
				_cancellationToken = cancellationToken;

				_inner = new CatchupAllSubscription(_subscriptionId, bus, startPosition ?? Position.Start, resolveLinks,
					eventFilter, user, readIndex, cancellationToken);
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
					_inner = new LiveStreamSubscription(_subscriptionId, _bus,
						OnLiveSubscriptionDropped, CurrentPosition, _resolveLinks, _eventFilter, _user,
						_cancellationToken);
				} else {
					_catchUpRestarted = true;
				}

				return await _inner.MoveNextAsync().ConfigureAwait(false);
			}

			public ValueTask DisposeAsync() => _inner.DisposeAsync();

			private async ValueTask OnLiveSubscriptionDropped(Position caughtUpPosition) {
				Log.Trace(
					"Live subscription {subscriptionId} to $all:{eventFilter} dropped, reading will resume after {caughtUpPosition}",
					_subscriptionId, _eventFilter, caughtUpPosition);

				await _inner.DisposeAsync().ConfigureAwait(false);

				_inner = new CatchupAllSubscription(_subscriptionId, _bus, caughtUpPosition, _resolveLinks,
					_eventFilter, _user, _readIndex, _cancellationToken);
			}

			private class CatchupAllSubscription : IAsyncEnumerator<ResolvedEvent> {
				private readonly Guid _subscriptionId;
				private readonly IPublisher _bus;
				private readonly bool _resolveLinks;
				private readonly IEventFilter _eventFilter;
				private readonly IPrincipal _user;
				private readonly CancellationTokenSource _disposedTokenSource;
				private readonly ConcurrentQueueWrapper<ResolvedEvent> _buffer;
				private readonly CancellationTokenRegistration _tokenRegistration;
				private Position _nextPosition;
				private ResolvedEvent _current;

				public ResolvedEvent Current => _current;

				public CatchupAllSubscription(Guid subscriptionId,
					IPublisher bus,
					Position position,
					bool resolveLinks,
					IEventFilter eventFilter,
					IPrincipal user,
					IReadIndex readIndex,
					CancellationToken cancellationToken) {
					if (bus == null) {
						throw new ArgumentNullException(nameof(bus));
					}

					if (eventFilter == null) {
						throw new ArgumentNullException(nameof(eventFilter));
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
					_eventFilter = eventFilter;
					_user = user;
					_disposedTokenSource = new CancellationTokenSource();
					_buffer = new ConcurrentQueueWrapper<ResolvedEvent>();
					_tokenRegistration = cancellationToken.Register(_disposedTokenSource.Dispose);
					Log.Info("Catch-up subscription {subscriptionId} to $all:{eventFilter} running...",
						_subscriptionId,
						_eventFilter);
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
						"Catch-up subscription {subscriptionId} to $all:{eventFilter} reading next page starting from {nextPosition}.",
						_subscriptionId, _eventFilter, _nextPosition);

					_bus.Publish(new ClientMessage.FilteredReadAllEventsForward(
						correlationId, correlationId, new CallbackEnvelope(OnMessage), commitPosition, preparePosition,
						32, _resolveLinks, false, 32, default, _eventFilter, _user));

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

						if (!(message is ClientMessage.FilteredReadAllEventsForwardCompleted completed)) {
							readNextSource.TrySetException(
								RpcExceptions.UnknownMessage<ClientMessage.FilteredReadAllEventsForwardCompleted>(
									message));
							return;
						}

						switch (completed.Result) {
							case FilteredReadAllResult.Success:
								foreach (var @event in completed.Events) {
									Log.Trace(
										"Catch-up subscription {subscriptionId} to $all received event {position}.",
										_subscriptionId, @event.OriginalPosition);
									_buffer.Enqueue(@event);
								}

								_nextPosition = Position.FromInt64(
									completed.NextPos.CommitPosition,
									completed.NextPos.PreparePosition);
								readNextSource.TrySetResult(completed.IsEndOfStream);
								return;
							case FilteredReadAllResult.AccessDenied:
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
				private readonly Position _caughtUpLastPosition;
				private readonly IEventFilter _eventFilter;
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
					Position caughtUpLastPosition,
					bool resolveLinks,
					IEventFilter eventFilter,
					IPrincipal user,
					CancellationToken cancellationToken) {
					if (bus == null) {
						throw new ArgumentNullException(nameof(bus));
					}

					if (onDropped == null) {
						throw new ArgumentNullException(nameof(onDropped));
					}

					if (eventFilter == null) {
						throw new ArgumentNullException(nameof(eventFilter));
					}

					_liveEventBuffer = new ConcurrentQueueWrapper<(ResolvedEvent resolvedEvent, Exception exception)>();
					_historicalEventBuffer = new Stack<(ResolvedEvent resolvedEvent, Exception exception)>();
					_subscriptionId = subscriptionId;
					_bus = bus;
					_onDropped = onDropped;
					_caughtUpLastPosition = caughtUpLastPosition;
					_eventFilter = eventFilter;
					_user = user;
					_subscriptionConfirmed = new TaskCompletionSource<Position>();
					_readHistoricalEventsCompleted = new TaskCompletionSource<bool>();
					_disposedTokenSource = new CancellationTokenSource();
					_tokenRegistration = cancellationToken.Register(_disposedTokenSource.Dispose);

					Log.Info("Live subscription {subscriptionId} to $all:{eventFilter} running...", _subscriptionId,
						eventFilter);

					bus.Publish(new ClientMessage.FilteredSubscribeToStream(Guid.NewGuid(), _subscriptionId,
						new CallbackEnvelope(OnSubscriptionMessage), subscriptionId, string.Empty, resolveLinks, user,
						eventFilter, 32));

					void OnSubscriptionMessage(Message message) {
						if (message is ClientMessage.NotHandled notHandled &&
						    RpcExceptions.TryHandleNotHandled(notHandled, out var ex)) {
							_subscriptionConfirmed.TrySetException(ex);
							return;
						}

						switch (message) {
							case ClientMessage.SubscriptionConfirmation confirmed:
								Log.Trace(
									"Live subscription {subscriptionId} to $all:{eventFilter} confirmed at {position}.",
									_subscriptionId, _eventFilter, Position.FromInt64(confirmed.LastIndexedPosition,
										confirmed.LastIndexedPosition));
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
								if (!_subscriptionConfirmed.Task.IsCompleted) {
									var fromPosition = Position.FromInt64(
										appeared.Event.OriginalPosition.Value.CommitPosition,
										appeared.Event.OriginalPosition.Value.PreparePosition);
									_subscriptionConfirmed.TrySetResult(fromPosition);
									ReadHistoricalEvents(fromPosition);
								}

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

						if (!(message is ClientMessage.FilteredReadAllEventsBackwardCompleted completed)) {
							_readHistoricalEventsCompleted.TrySetException(
								RpcExceptions.UnknownMessage<ClientMessage.FilteredReadAllEventsBackwardCompleted>(
									message));
							return;
						}

						switch (completed.Result) {
							case FilteredReadAllResult.Success:
								if (completed.Events.Length == 0) {
									Log.Trace(
										"Live subscription {subscriptionId} to $all:{eventFilter} caught up.",
										_subscriptionId, _eventFilter);
									_readHistoricalEventsCompleted.TrySetResult(true);
									return;
								}

								foreach (var @event in completed.Events) {
									var position = Position.FromInt64(@event.OriginalPosition.Value.CommitPosition,
										@event.OriginalPosition.Value.PreparePosition);
									if (position <= _caughtUpLastPosition) {
										Log.Trace(
											"Live subscription {subscriptionId} to $all:{eventFilter} caught up at {position}.",
											_subscriptionId, _eventFilter, position);
										_readHistoricalEventsCompleted.TrySetResult(true);
										return;
									}

									_historicalEventBuffer.Push((@event, null));
								}

								ReadHistoricalEvents(Position.FromInt64(
									completed.NextPos.CommitPosition,
									completed.NextPos.PreparePosition));
								return;
							case FilteredReadAllResult.AccessDenied:
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
							"Live subscription {subscriptionId} to $all:{eventFilter} loading any missed events ending with {fromPosition}.",
							_subscriptionId, eventFilter, fromPosition);

						var correlationId = Guid.NewGuid();
						var (commitPosition, preparePosition) = fromPosition.ToInt64();
						bus.Publish(new ClientMessage.FilteredReadAllEventsBackward(correlationId, correlationId,
							new CallbackEnvelope(OnHistoricalEventsMessage), commitPosition, preparePosition, 32,
							resolveLinks, false, 32, null, eventFilter, user));
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
							Log.Warn("Live subscription {subscriptionId} to $all:{eventFilter} buffer is full.",
								_subscriptionId, _eventFilter);

							_liveEventBuffer.Clear();
							_historicalEventBuffer.Clear();
							await _onDropped(position).ConfigureAwait(false);
							return false;
						}

						if (livePosition > position) {
							_current = historicalEvent;
							Log.Trace(
								"Live subscription {subscriptionId} to $all:{eventFilter} received event {position} historically.",
								_subscriptionId, _eventFilter, position);
							return true;
						}
					}

					var delay = 1;

					while (!_liveEventBuffer.TryDequeue(out _)) {
						await Task.Delay(Math.Max(delay *= 2, 50), _disposedTokenSource.Token).ConfigureAwait(false);
					}

					var (resolvedEvent, exception) = _;

					if (exception != null) {
						throw exception;
					}

					_current = resolvedEvent;
					Log.Trace(
						"Live subscription {subscriptionId} to $all:{eventFilter} received event {position} live.",
						_subscriptionId, _eventFilter, resolvedEvent.OriginalPosition);
					return true;
				}

				public ValueTask DisposeAsync() {
					Log.Info("Live subscription {subscriptionId} to $all:{eventFilter} disposed.", _subscriptionId,
						_eventFilter);
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
