using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Tests.Helpers;
using Xunit;
using EventStore.Core.Services.UserManagement;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services;
using EventStore.Core.Data;

namespace EventStore.Core.Tests.Replication.ReadStream {
	public static class ReplicationTestHelper {
		private static TimeSpan _timeout = TimeSpan.FromSeconds(8);

		public static Task<ClientMessage.WriteEventsCompleted> WriteEvent(MiniClusterNode node, Event[] events,
			string streamId) {
			var writeResultSource = new TaskCompletionSource<ClientMessage.WriteEventsCompleted>();
			node.Node.MainQueue.Publish(new ClientMessage.WriteEvents(Guid.NewGuid(), Guid.NewGuid(),
				new CallbackEnvelope(msg => {
					if (msg is ClientMessage.WriteEventsCompleted completed) {
						writeResultSource.TrySetResult(completed);
					} else {
						writeResultSource.TrySetException(new InvalidOperationException(
							$"Failed to write events. Expected {nameof(ClientMessage.WriteEventsCompleted)}; received {msg.GetType().Name}"));
					}
				}), false, streamId, -1, events,
				SystemAccount.Principal, SystemUsers.Admin, SystemUsers.DefaultAdminPassword));

			return writeResultSource.Task.WithTimeout(_timeout);
		}

		public static Task<ClientMessage.ReadAllEventsForwardCompleted> ReadAllEventsForward(MiniClusterNode node,
			long position) {
			var source = new TaskCompletionSource<ClientMessage.ReadAllEventsForwardCompleted>();
			while (!source.Task.IsCompleted) {
				var read = new ClientMessage.ReadAllEventsForward(Guid.NewGuid(), Guid.NewGuid(), new CallbackEnvelope(
						msg => {
							if (msg is ClientMessage.ReadAllEventsForwardCompleted completed) {
								if (completed.Result == ReadAllResult.Error) {
									source.TrySetException(
										new InvalidOperationException(
											$"Failed to read forwards. Read result error: {completed.Error}"));
								} else {
									if (completed.NextPos.CommitPosition > position) {
										source.TrySetResult(completed);
									}
								}
							} else {
								source.TrySetException(new InvalidOperationException(
									$"Failed to read forwards. Expected {nameof(ClientMessage.ReadAllEventsForwardCompleted)}; received {msg.GetType().Name}"));
							}
						}),
					0, 0, 100, false, false, null, SystemAccount.Principal);
				node.Node.MainQueue.Publish(read);
			}

			return source.Task.WithTimeout(_timeout);
		}

		public static Task<ClientMessage.ReadAllEventsBackwardCompleted> ReadAllEventsBackward(MiniClusterNode node,
			long position) {
			var source = new TaskCompletionSource<ClientMessage.ReadAllEventsBackwardCompleted>();
			while (!source.Task.IsCompleted) {
				var read = new ClientMessage.ReadAllEventsBackward(Guid.NewGuid(), Guid.NewGuid(), new CallbackEnvelope(
						msg => {
							if (msg is ClientMessage.ReadAllEventsBackwardCompleted completed) {
								if (completed.Result == ReadAllResult.Error) {
									source.TrySetException(
										new InvalidOperationException(
											$"Failed to read backwards. Read result error: {completed.Error}"));
								} else {
									if (completed.NextPos.CommitPosition < position) {
										source.TrySetResult(completed);
									}
								}
							} else {
								source.TrySetException(new InvalidOperationException(
									$"Failed to read backwards. Expected {nameof(ClientMessage.ReadAllEventsBackwardCompleted)}; received {msg.GetType().Name}"));
							}
						}),
					-1, -1, 100, false, false, null, SystemAccount.Principal);
				node.Node.MainQueue.Publish(read);
			}

			return source.Task.WithTimeout(_timeout);
		}

		public static Task<ClientMessage.ReadStreamEventsForwardCompleted> ReadStreamEventsForward(MiniClusterNode node,
			string streamId) {
			var source = new TaskCompletionSource<ClientMessage.ReadStreamEventsForwardCompleted>();
			var read = new ClientMessage.ReadStreamEventsForward(Guid.NewGuid(), Guid.NewGuid(), new CallbackEnvelope(
					msg => {
						if (msg is ClientMessage.ReadStreamEventsForwardCompleted completed) {
							if (completed.Result == ReadStreamResult.Error) {
								source.TrySetException(
									new InvalidOperationException(
										$"Failed to read forwards. Read result error: {completed.Error}"));
							} else {
								source.TrySetResult(completed);
							}
						} else {
							source.TrySetException(new InvalidOperationException(
								$"Failed to read forwards. Expected {nameof(ClientMessage.ReadStreamEventsForwardCompleted)}; received {msg.GetType().Name}"));
						}
					}), streamId, 0, 10,
				false, false, null, SystemAccount.Principal);
			node.Node.MainQueue.Publish(read);

			return source.Task.WithTimeout(_timeout);
		}

		public static Task<ClientMessage.ReadStreamEventsBackwardCompleted> ReadStreamEventsBackward(
			MiniClusterNode node,
			string streamId) {
			var source = new TaskCompletionSource<ClientMessage.ReadStreamEventsBackwardCompleted>();
			var read = new ClientMessage.ReadStreamEventsBackward(Guid.NewGuid(), Guid.NewGuid(), new CallbackEnvelope(
					msg => {
						if (msg is ClientMessage.ReadStreamEventsBackwardCompleted completed) {
							if (completed.Result == ReadStreamResult.Error) {
								source.TrySetException(
									new InvalidOperationException(
										$"Failed to read forwards. Read result error: {completed.Error}"));
							} else {
								source.TrySetResult(completed);
							}
						} else {
							source.TrySetException(new InvalidOperationException(
								$"Failed to read backwards. Expected {nameof(ClientMessage.ReadStreamEventsBackwardCompleted)}; received {msg.GetType().Name}"));
						}
					}), streamId, 9, 10,
				false, false, null, SystemAccount.Principal);
			node.Node.MainQueue.Publish(read);

			return source.Task.WithTimeout(_timeout);
		}

		public static Task<ClientMessage.ReadEventCompleted> ReadEvent(MiniClusterNode node, string streamId,
			long eventNumber) {
			var source = new TaskCompletionSource<ClientMessage.ReadEventCompleted>();
			var read = new ClientMessage.ReadEvent(Guid.NewGuid(), Guid.NewGuid(), new CallbackEnvelope(msg => {
					if (msg is ClientMessage.ReadEventCompleted completed) {
						if (completed.Result == ReadEventResult.Error) {
							source.TrySetException(
								new InvalidOperationException(
									$"Failed to read event. Read result error: {completed.Error}"));
						} else {
							source.TrySetResult(completed);
						}
					} else {
						source.TrySetException(new InvalidOperationException(
							$"Failed to read event. Expected {nameof(ClientMessage.ReadEventCompleted)}; received {msg.GetType().Name}"));
					}
				}), streamId, eventNumber,
				false, false, SystemAccount.Principal);
			node.Node.MainQueue.Publish(read);

			return source.Task.WithTimeout(_timeout);
		}
	}
}
