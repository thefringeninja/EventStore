using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.projection_manager_response_reader {
	public class
		when_timeout_received_after_read_succeeds : specification_with_projection_manager_response_reader_started {
		private Guid _projectionId;
		private Guid _readStreamEventsCorrelationId;
		private string _projectionsMasterStream = "$projections-$master";

		protected override IEnumerable<WhenStep> When() {
			AllReadsTimeOut();
			Consumer.HandledMessages.Clear();

			_projectionId = Guid.NewGuid();
			yield return
				CreateWriteEvent(
					_projectionsMasterStream,
					"$stopped",
					@"{
                        ""id"":""" + _projectionId.ToString("N") + @""",
                    }",
					null,
					true);
			var readStreamMessage = Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.LastOrDefault(x => x.EventStreamId == _projectionsMasterStream);
			Assert.NotNull(readStreamMessage);

			_readStreamEventsCorrelationId = readStreamMessage.CorrelationId;
			readStreamMessage.Envelope.ReplyWith(new ClientMessage.ReadStreamEventsForwardCompleted(
				_readStreamEventsCorrelationId, _projectionsMasterStream, readStreamMessage.FromEventNumber,
				readStreamMessage.MaxCount,
				ReadStreamResult.Success, new ResolvedEvent[0], null, false, "", readStreamMessage.FromEventNumber,
				readStreamMessage.FromEventNumber,
				true, 1000));
		}

		[Fact]
		public void does_not_issue_a_new_read() {
			_commandReader.Handle(new ProjectionManagementMessage.Internal.ReadTimeout(_readStreamEventsCorrelationId,
				_projectionsMasterStream));

			var response = HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
				.Last(x => x.EventStreamId == _projectionsMasterStream);
			Assert.NotNull(response);
			Assert.Equal(_readStreamEventsCorrelationId, response.CorrelationId);
		}
	}
}
