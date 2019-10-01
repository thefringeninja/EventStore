using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Messages.Persisted.Responses;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_response_writer {
	public class when_handling_result_report_message : specification_with_projection_core_service_response_writer {
		private Guid _projectionId;
		private string _result;
		private string _partition;
		private Guid _correlationId;
		private CheckpointTag _position;

		protected override void Given() {
			_correlationId = Guid.NewGuid();
			_projectionId = Guid.NewGuid();
			_result = "{\"a\":1}";
			_partition = "partition";
			_position = CheckpointTag.FromStreamPosition(1, "stream", 10);
		}

		protected override void When() {
			_sut.Handle(
				new CoreProjectionStatusMessage.ResultReport(
					_correlationId,
					_projectionId,
					_partition,
					_result,
					_position));
		}

		[Fact]
		public void publishes_result_report_response() {
			var command = AssertParsedSingleCommand<ResultReport>("$result");
			Assert.Equal(_projectionId.ToString("N"), command.Id);
			Assert.Equal(_correlationId.ToString("N"), command.CorrelationId);
			Assert.Equal(_result, command.Result);
			Assert.Equal(_partition, command.Partition);
			Assert.Equal(_position, command.Position);
		}
	}
}
