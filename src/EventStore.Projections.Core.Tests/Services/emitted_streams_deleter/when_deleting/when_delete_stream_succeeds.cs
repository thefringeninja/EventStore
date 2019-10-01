using System;
using System.Collections.Generic;
using System.Threading;
using EventStore.Core.Messages;
using Xunit;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.emitted_streams_deleter.when_deleting {
	public class when_delete_stream_succeeds : with_emitted_stream_deleter {
		protected Action _onDeleteStreamCompleted;
		private readonly ManualResetEventSlim _mre = new ManualResetEventSlim();
		private readonly List<ClientMessage.DeleteStream> _deleteMessages = new List<ClientMessage.DeleteStream>();

		public override void When() {
			_onDeleteStreamCompleted = () => { _mre.Set(); };

			_deleter.DeleteEmittedStreams(_onDeleteStreamCompleted);
		}

		public override void Handle(ClientMessage.DeleteStream message) {
			_deleteMessages.Add(message);
			message.Envelope.ReplyWith(new ClientMessage.DeleteStreamCompleted(
				message.CorrelationId, OperationResult.Success, String.Empty));
		}

		[Fact]
		public void should_have_deleted_the_tracked_emitted_stream() {
			if (!_mre.Wait(10000)) {
				throw new Exception("Timed out waiting for event to be deleted");
			}

			Assert.Equal(_testStreamName, _deleteMessages[0].EventStreamId);
			Assert.Equal(_checkpointName, _deleteMessages[1].EventStreamId);
		}
	}
}
