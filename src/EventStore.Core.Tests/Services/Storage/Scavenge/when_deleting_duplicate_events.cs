using System.Linq;
using EventStore.Core.Data;
using Xunit;

namespace EventStore.Core.Tests.Services.Storage.Scavenge {
	public class when_deleting_duplicate_events : ReadIndexTestScenario {
		private EventRecord _event1;
		private EventRecord _event2;
		private EventRecord _event3;
		private EventRecord _event4;
		private EventRecord _event5;
		private EventRecord _event6;
		private EventRecord _event7;
		private EventRecord _event8;

		public when_deleting_duplicate_events() : base(
			indexBitnessVersion: EventStore.Core.Index.PTableVersions.IndexV1, performAdditionalChecks: false) {
		}

		protected override void WriteTestScenario() {
			_event1 = WriteSingleEvent("account--696193173", 0, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("account--696193173", 0, new string('.', 3000), retryOnFail: true);

			_event2 = WriteSingleEvent("LPN-FC002_LPK51001", 0, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("LPN-FC002_LPK51001", 0, new string('.', 3000), retryOnFail: true);

			_event3 = WriteSingleEvent("account--696193173", 1, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("account--696193173", 1, new string('.', 3000), retryOnFail: true);

			_event4 = WriteSingleEvent("LPN-FC002_LPK51001", 1, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("LPN-FC002_LPK51001", 1, new string('.', 3000), retryOnFail: true);

			_event5 = WriteSingleEvent("account--696193173", 2, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("account--696193173", 2, new string('.', 3000), retryOnFail: true);

			_event6 = WriteSingleEvent("LPN-FC002_LPK51001", 2, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("LPN-FC002_LPK51001", 2, new string('.', 3000), retryOnFail: true);

			_event7 = WriteSingleEvent("account--696193173", 3, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("account--696193173", 3, new string('.', 3000), retryOnFail: true);

			_event8 = WriteSingleEvent("LPN-FC002_LPK51001", 3, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("LPN-FC002_LPK51001", 3, new string('.', 3000), retryOnFail: true);

			WriteSingleEvent("RandomStream", 0, new string('.', 3000), retryOnFail: true);
			WriteSingleEvent("RandomStream", 1, new string('.', 3000), retryOnFail: true);

			Scavenge(completeLast: false, mergeChunks: false);
		}

		[Fact]
		public void read_all_events_forward_does_not_return_duplicate() {
			var events = ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100).Records.Select(r => r.Event).ToArray();
			Assert.Equal(11, events.Length);
			Assert.Equal(_event1, events[0]);
			Assert.Equal(_event2, events[1]);
			Assert.Equal(_event3, events[2]);
			Assert.Equal(_event4, events[3]);
			Assert.Equal(_event5, events[4]);
			Assert.Equal(_event6, events[5]);
			Assert.Equal(_event7, events[6]);
			Assert.Equal(_event8, events[7]);
		}
	}
}
