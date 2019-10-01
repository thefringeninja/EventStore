using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Bus;
using EventStore.Core.Helpers;
using EventStore.Core.Services.TimerService;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.command_reader_response_reader_integration {
	public class
		when_command_reader_times_out_reading_control_stream_on_startup :
			specification_with_command_reader_and_response_reader {
		private Guid _epochId = Guid.NewGuid();

		protected override void Given() {
			_numberOfWorkers = 1;
			var controlStream = ProjectionNamesBuilder.BuildControlStreamName(_epochId);
			TimeOutReadToStreamOnce(controlStream);

			int timeoutCount = 0;
			_bus.Subscribe(new AdHocHandler<TimerMessage.Schedule>(msg => {
				if (msg.ReplyMessage is IODispatcherDelayedMessage && timeoutCount <= 1) {
					// Only the second read should time out as the first is of the control stream
					if (timeoutCount == 1) {
						msg.Reply();
					}

					timeoutCount++;
				}
			}));
			base.Given();
		}

		protected override IEnumerable<WhenStep> When() {
			yield return new WhenStep(
				new ProjectionCoreServiceMessage.StartCore(_epochId),
				new ProjectionManagementMessage.Starting(_epochId));
		}

		[Fact]
		public void should_send_reader_ready() {
			Assert.Equal(1, Consumer.HandledMessages.OfType<ProjectionManagementMessage.ReaderReady>().Count());
		}
	}
}
