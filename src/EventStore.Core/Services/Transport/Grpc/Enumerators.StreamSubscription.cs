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
using EventStore.Client;
using IReadIndex = EventStore.Core.Services.Storage.ReaderIndex.IReadIndex;

namespace EventStore.Core.Services.Transport.Grpc {
	internal static partial class Enumerators {
		public class StreamSubscription : IAsyncEnumerator<ResolvedEvent> {
			private static readonly ILogger Log = LogManager.GetLoggerFor<StreamSubscription>();

			private readonly Guid _subscriptionId;
			private readonly IPublisher _bus;
			private readonly string _streamName;
			private readonly StreamRevision? _startRevision;
			private readonly bool _resolveLinks;
			private readonly IPrincipal _user;
			private readonly IReadIndex _readIndex;
			private readonly CancellationToken _cancellationToken;
			private IAsyncEnumerator<ResolvedEvent> _inner;

			private bool _catchUpRestarted;

			public ResolvedEvent Current => _inner.Current;

			private StreamRevision CurrentRevision =>
				Current.OriginalEvent == null
					? StreamRevision.Start
					: StreamRevision.FromInt64(Current.OriginalEvent.EventNumber);

			public StreamSubscription(
				IPublisher bus,
				string streamName,
				StreamRevision? startRevision,
				bool resolveLinks,
				IPrincipal user,
				IReadIndex readIndex,
				CancellationToken cancellationToken) {
				if (bus == null) {
					throw new ArgumentNullException(nameof(bus));
				}

				if (streamName == null) {
					throw new ArgumentNullException(nameof(streamName));
				}

				if (readIndex == null) {
					throw new ArgumentNullException(nameof(readIndex));
				}

				_subscriptionId = Guid.NewGuid();
				_bus = bus;
				_streamName = streamName;
				_startRevision = startRevision == StreamRevision.End ? StreamRevision.Start : startRevision;
				_resolveLinks = resolveLinks;
				_user = user;
				_readIndex = readIndex;
				_cancellationToken = cancellationToken;

				_inner = startRevision == StreamRevision.End
					? (IAsyncEnumerator<ResolvedEvent>)new LiveStreamSubscription(_subscriptionId, _bus,
						OnLiveSubscriptionDropped, _streamName,
						StreamRevision.FromInt64(readIndex.GetStreamLastEventNumber(_streamName)), _resolveLinks,
						_user, _cancellationToken)
					: new CatchupStreamSubscription(_subscriptionId, bus, streamName,
						startRevision + 1 ?? StreamRevision.Start, resolveLinks, user, readIndex, cancellationToken);
			}

			public async ValueTask<bool> MoveNextAsync() {
				if (!await MoveNextCoreAsync().ConfigureAwait(false)) {
					return false;
				}

				if (!_startRevision.HasValue) {
					return true;
				}

				if (_startRevision.Value > CurrentRevision) {
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

					_inner = new LiveStreamSubscription(_subscriptionId, _bus, OnLiveSubscriptionDropped, _streamName,
						CurrentRevision, _resolveLinks, _user, _cancellationToken);
				} else {
					_catchUpRestarted = true;
				}

				return await _inner.MoveNextAsync().ConfigureAwait(false);
			}

			public ValueTask DisposeAsync() => _inner.DisposeAsync();

			private async ValueTask OnLiveSubscriptionDropped(StreamRevision caughtUpRevision) {
				Log.Trace(
					"Live subscription {subscriptionId} to {streamName} dropped, reading will resume after {caughtUpRevision}",
					_subscriptionId, _streamName, caughtUpRevision);
				await _inner.DisposeAsync().ConfigureAwait(false);

				_inner = new CatchupStreamSubscription(_subscriptionId, _bus, _streamName, caughtUpRevision,
					_resolveLinks, _user, _readIndex, _cancellationToken);
			}

			private class CatchupStreamSubscription : IAsyncEnumerator<ResolvedEvent> {
				private readonly Guid _subscriptionId;
				private readonly IPublisher _bus;
				private readonly string _streamName;
				private readonly StreamRevision _startRevision;
				private readonly bool _resolveLinks;
				private readonly IPrincipal _user;
				private readonly CancellationTokenSource _disposedTokenSource;
				private readonly ConcurrentQueue<ResolvedEvent> _buffer;
				private readonly CancellationTokenRegistration _tokenRegistration;
				private StreamRevision _nextRevision;
				private ResolvedEvent _current;

				public ResolvedEvent Current => _current;

				public CatchupStreamSubscription(Guid subscriptionId,
					IPublisher bus,
					string streamName,
					StreamRevision startRevision,
					bool resolveLinks,
					IPrincipal user,
					IReadIndex readIndex,
					CancellationToken cancellationToken) {
					if (bus == null) {
						throw new ArgumentNullException(nameof(bus));
					}

					if (streamName == null) {
						throw new ArgumentNullException(nameof(streamName));
					}

					if (readIndex == null) {
						throw new ArgumentNullException(nameof(readIndex));
					}

					_subscriptionId = subscriptionId;
					_bus = bus;
					_streamName = streamName;
					_startRevision = startRevision;
					_nextRevision = startRevision == StreamRevision.End
						? StreamRevision.FromInt64(readIndex.GetStreamLastEventNumber(_streamName) + 1L)
						: startRevision;
					_resolveLinks = resolveLinks;
					_user = user;
					_disposedTokenSource = new CancellationTokenSource();
					_buffer = new ConcurrentQueue<ResolvedEvent>();
					_tokenRegistration = cancellationToken.Register(_disposedTokenSource.Dispose);
					Log.Info("Catch-up subscription {subscriptionId} to {streamName} running...", _subscriptionId,
						streamName);
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

					Log.Trace(
						"Catch-up subscription {_subscriptionId} to {_streamName} reading next page starting from {_nextRevision}",
						_subscriptionId, _streamName, _nextRevision);

					_bus.Publish(new ClientMessage.ReadStreamEventsForward(
						correlationId, correlationId, new CallbackEnvelope(OnMessage), _streamName,
						_nextRevision.ToInt64(), ReadBatchSize, _resolveLinks, false, default, _user));

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

						if (!(message is ClientMessage.ReadStreamEventsForwardCompleted completed)) {
							readNextSource.TrySetException(
								RpcExceptions.UnknownMessage<ClientMessage.ReadStreamEventsForwardCompleted>(message));
							return;
						}

						switch (completed.Result) {
							case ReadStreamResult.Success:
								foreach (var @event in completed.Events) {
									if (StreamRevision.FromInt64(@event.OriginalEvent.EventNumber) < _startRevision) {
										continue;
									}

									_buffer.Enqueue(@event);
								}


								_nextRevision = StreamRevision.FromInt64(completed.NextEventNumber);
								readNextSource.TrySetResult(completed.IsEndOfStream);
								return;
							case ReadStreamResult.NoStream:
								readNextSource.TrySetException(RpcExceptions.StreamNotFound(_streamName));
								return;
							case ReadStreamResult.StreamDeleted:
								readNextSource.TrySetException(RpcExceptions.StreamDeleted(_streamName));
								return;
							case ReadStreamResult.AccessDenied:
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
				private readonly string _streamName;
				private readonly StreamRevision _caughtUpRevision;
				private readonly IPrincipal _user;
				private readonly Func<StreamRevision, ValueTask> _onDropped;
				private readonly TaskCompletionSource<StreamRevision> _subscriptionConfirmed;
				private readonly TaskCompletionSource<bool> _readHistoricalEventsCompleted;
				private readonly CancellationTokenRegistration _tokenRegistration;
				private readonly CancellationTokenSource _disposedTokenSource;

				private ResolvedEvent _current;

				public ResolvedEvent Current => _current;

				public LiveStreamSubscription(Guid subscriptionId,
					IPublisher bus,
					Func<StreamRevision, ValueTask> onDropped,
					string streamName,
					StreamRevision caughtUpRevision,
					bool resolveLinks,
					IPrincipal user,
					CancellationToken cancellationToken) {
					if (bus == null) {
						throw new ArgumentNullException(nameof(bus));
					}

					if (streamName == null) {
						throw new ArgumentNullException(nameof(streamName));
					}

					if (onDropped == null) {
						throw new ArgumentNullException(nameof(onDropped));
					}

					_liveEventBuffer = new ConcurrentQueueWrapper<(ResolvedEvent resolvedEvent, Exception exception)>();
					_historicalEventBuffer = new Stack<(ResolvedEvent resolvedEvent, Exception exception)>();
					_subscriptionId = subscriptionId;
					_bus = bus;
					_onDropped = onDropped;
					_streamName = streamName;
					_caughtUpRevision = caughtUpRevision;
					_user = user;
					_subscriptionConfirmed = new TaskCompletionSource<StreamRevision>();
					_readHistoricalEventsCompleted = new TaskCompletionSource<bool>();
					_disposedTokenSource = new CancellationTokenSource();
					_tokenRegistration = cancellationToken.Register(_disposedTokenSource.Dispose);

					Log.Info("Live subscription {subscriptionId} to {streamName} running...", subscriptionId,
						streamName);

					bus.Publish(new ClientMessage.SubscribeToStream(Guid.NewGuid(), _subscriptionId,
						new CallbackEnvelope(OnSubscriptionMessage), subscriptionId, streamName, resolveLinks, user));

					void OnSubscriptionMessage(Message message) {
						if (message is ClientMessage.NotHandled notHandled &&
						    RpcExceptions.TryHandleNotHandled(notHandled, out var ex)) {
							_subscriptionConfirmed.TrySetException(ex);
							return;
						}

						switch (message) {
							case ClientMessage.SubscriptionConfirmation confirmed:
								Log.Trace(
									"Live subscription {subscriptionId} to {streamName} confirmed at {fromRevision}.",
									_subscriptionId, _streamName,
									StreamRevision.FromInt64(confirmed.LastEventNumber.Value));

								return;
							case ClientMessage.SubscriptionDropped dropped:
								switch (dropped.Reason) {
									case SubscriptionDropReason.AccessDenied:
										Fail(RpcExceptions.AccessDenied());
										return;
									case SubscriptionDropReason.NotFound:
										Fail(RpcExceptions.StreamNotFound(streamName));
										return;
									default:
										Fail(RpcExceptions.UnknownError(dropped.Reason));
										return;
								}
							case ClientMessage.StreamEventAppeared appeared:
								if (!_subscriptionConfirmed.Task.IsCompleted) {
									var fromRevision = StreamRevision.FromInt64(appeared.Event.OriginalEventNumber);
									_subscriptionConfirmed.TrySetResult(fromRevision);
									ReadHistoricalEvents(fromRevision);
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

						if (!(message is ClientMessage.ReadStreamEventsBackwardCompleted completed)) {
							_readHistoricalEventsCompleted.TrySetException(
								RpcExceptions.UnknownMessage<ClientMessage.ReadStreamEventsBackwardCompleted>(message));
							return;
						}

						switch (completed.Result) {
							case ReadStreamResult.Success:
								if (completed.Events.Length == 0) {
									Log.Trace("Live subscription {subscriptionId} to {streamName} caught up.",
										_subscriptionId, streamName);
									_readHistoricalEventsCompleted.TrySetResult(true);
									return;
								}

								foreach (var @event in completed.Events) {
									var streamRevision = StreamRevision.FromInt64(@event.OriginalEvent.EventNumber);
									if (streamRevision <= _caughtUpRevision) {
										Log.Trace(
											"Live subscription {subscriptionId} to {streamName} caught up.",
											_subscriptionId, streamName);
										_readHistoricalEventsCompleted.TrySetResult(true);
										return;
									}

									_historicalEventBuffer.Push((@event, null));
								}

								ReadHistoricalEvents(StreamRevision.FromInt64(completed.NextEventNumber));
								return;
							case ReadStreamResult.NoStream:
								_readHistoricalEventsCompleted.TrySetException(
									RpcExceptions.StreamNotFound(streamName));
								return;
							case ReadStreamResult.StreamDeleted:
								_readHistoricalEventsCompleted.TrySetException(RpcExceptions.StreamDeleted(streamName));
								return;
							case ReadStreamResult.AccessDenied:
								_readHistoricalEventsCompleted.TrySetException(RpcExceptions.AccessDenied());
								return;
							default:
								_readHistoricalEventsCompleted.TrySetException(
									RpcExceptions.UnknownError(completed.Result));
								return;
						}
					}

					void ReadHistoricalEvents(StreamRevision fromStreamRevision) {
						Log.Trace(
							"Live subscription {subscriptionId} to {streamName} loading any missed events starting at {fromStreamRevision}",
							subscriptionId,
							streamName,
							fromStreamRevision);

						var correlationId = Guid.NewGuid();
						bus.Publish(new ClientMessage.ReadStreamEventsBackward(correlationId, correlationId,
							new CallbackEnvelope(OnHistoricalEventsMessage), streamName, fromStreamRevision.ToInt64(),
							ReadBatchSize, resolveLinks, false, null, user));
					}
				}

				public async ValueTask<bool> MoveNextAsync() {
					(ResolvedEvent, Exception) _;

					await Task.WhenAll(_subscriptionConfirmed.Task, _readHistoricalEventsCompleted.Task)
						.ConfigureAwait(false);

					var liveStreamRevision = _subscriptionConfirmed.Task.Result;

					if (_historicalEventBuffer.TryPop(out _)) {
						var (historicalEvent, historicalException) = _;

						if (historicalException != null) {
							throw historicalException;
						}

						var streamRevision = StreamRevision.FromInt64(historicalEvent.OriginalEvent.EventNumber);

						if (_liveEventBuffer.Count > MaxLiveEventBufferCount) {
							Log.Warn("Live subscription {subscriptionId} to {streamName} buffer is full.",
								_subscriptionId, _streamName);

							_liveEventBuffer.Clear();
							_historicalEventBuffer.Clear();

							await _onDropped(streamRevision)
								.ConfigureAwait(false);
							return false;
						}

						if (liveStreamRevision > streamRevision) {
							_current = historicalEvent;
							Log.Trace(
								"Live subscription {subscriptionId} to {streamName} received event {eventNumber} historically.",
								_subscriptionId, _streamName, historicalEvent.OriginalEventNumber);
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

					_current = resolvedEvent;
					Log.Trace("Live subscription {subscriptionId} to {streamName} received event {eventNumber} live.",
						_subscriptionId, _streamName, resolvedEvent.OriginalEventNumber);
					return true;
				}

				public ValueTask DisposeAsync() {
					Log.Info("Live subscription {subscriptionId} to {streamName} disposed.", _subscriptionId,
						_streamName);
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
