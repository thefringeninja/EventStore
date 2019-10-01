using System;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;

namespace EventStore.Core.Tests.AwakeService {
	public class when_handling_comitted_event {
		private Core.Services.AwakeReaderService.AwakeService _it;
		private EventRecord _eventRecord;
		private StorageMessage.EventCommitted _eventCommitted;
		private Exception _exception;

		public when_handling_comitted_event() {
			_exception = null;
			Given();
			When();
		}

		private void Given() {
			_it = new Core.Services.AwakeReaderService.AwakeService();

			_eventRecord = new EventRecord(
				10,
				new PrepareLogRecord(
					500, Guid.NewGuid(), Guid.NewGuid(), 500, 0, "Stream", 99, DateTime.UtcNow, PrepareFlags.Data,
					"event", new byte[0], null));
			_eventCommitted = new StorageMessage.EventCommitted(1000, _eventRecord, isTfEof: true);
		}

		private void When() {
			try {
				_it.Handle(_eventCommitted);
			} catch (Exception ex) {
				_exception = ex;
			}
		}

		[Fact]
		public void it_is_handled() {
			Assert.Null(_exception);
		}
	}
}
