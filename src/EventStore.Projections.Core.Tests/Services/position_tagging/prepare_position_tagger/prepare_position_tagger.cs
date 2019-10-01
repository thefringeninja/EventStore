using System;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.prepare_position_tagger {
	public class prepare_position_tagger {
		private ReaderSubscriptionMessage.CommittedEventDistributed _zeroEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _firstEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _secondEvent;

		public prepare_position_tagger() {
			_zeroEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(10, 0), "stream", 0, false, Guid.NewGuid(), "StreamCreated", false,
				new byte[0], new byte[0]);
			_firstEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(0, 20), "stream", 1, false, Guid.NewGuid(), "Data", true,
				Helper.UTF8NoBom.GetBytes("{}"), new byte[0]);
			_secondEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(50, 40), "stream", 2, false, Guid.NewGuid(), "Data", true,
				Helper.UTF8NoBom.GetBytes("{}"), new byte[0]);
		}

		[Fact]
		public void can_be_created() {
			new PreparePositionTagger(0);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_after_case() {
			var t = new PreparePositionTagger(0);
			var result = t.IsMessageAfterCheckpointTag(CheckpointTag.FromPreparePosition(0, 10), _firstEvent);
			Assert.True(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_before_case() {
			var t = new PreparePositionTagger(0);
			var result = t.IsMessageAfterCheckpointTag(CheckpointTag.FromPreparePosition(0, 40), _firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_equal_case() {
			var t = new PreparePositionTagger(0);
			var result = t.IsMessageAfterCheckpointTag(CheckpointTag.FromPreparePosition(0, 20), _firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void prepare_position_checkpoint_tag_is_compatible() {
			var t = new PreparePositionTagger(0);
			Assert.True(t.IsCompatible(CheckpointTag.FromPreparePosition(0, 500)));
		}

		[Fact]
		public void tf_position_checkpoint_tag_is_incompatible() {
			var t = new PreparePositionTagger(0);
			Assert.False(t.IsCompatible(CheckpointTag.FromPosition(0, 510, 500)));
		}

		[Fact]
		public void stream_checkpoint_tag_is_incompatible() {
			var t = new PreparePositionTagger(0);
			Assert.False(t.IsCompatible(CheckpointTag.FromStreamPosition(0, "stream2", 100)));
		}

		[Fact]
		public void zero_position_tag_is_before_first_event_possible() {
			var t = new PreparePositionTagger(0);
			var zero = t.MakeZeroCheckpointTag();

			var zeroFromEvent = t.MakeCheckpointTag(zero, _zeroEvent);

			Assert.True(zeroFromEvent > zero);
		}

		[Fact]
		public void produced_checkpoint_tags_are_correctly_ordered() {
			var t = new PreparePositionTagger(0);
			var zero = t.MakeZeroCheckpointTag();

			var zeroEvent = t.MakeCheckpointTag(zero, _zeroEvent);
			var zeroEvent2 = t.MakeCheckpointTag(zeroEvent, _zeroEvent);
			var first = t.MakeCheckpointTag(zeroEvent2, _firstEvent);
			var second = t.MakeCheckpointTag(first, _secondEvent);
			var second2 = t.MakeCheckpointTag(zero, _secondEvent);

			Assert.True(zeroEvent > zero);
			Assert.True(first > zero);
			Assert.True(second > first);

			Assert.Equal(zeroEvent2, zeroEvent);
			Assert.Equal(second, second2);
		}
	}
}
