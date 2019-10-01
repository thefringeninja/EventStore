using System;
using System.Collections.Generic;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.stream_position_tagger {
	public class stream_position_tagger {
		private ReaderSubscriptionMessage.CommittedEventDistributed _zeroEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _firstEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _secondEvent;

		public stream_position_tagger() {
			_zeroEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(10, 0), "stream1", 0, false, Guid.NewGuid(), "StreamCreated", false,
				new byte[0], new byte[0]);
			_firstEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(30, 20), "stream1", 1, false, Guid.NewGuid(), "Data", true,
				Helper.UTF8NoBom.GetBytes("{}"), new byte[0]);
			_secondEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(50, 40), "stream1", 2, false, Guid.NewGuid(), "Data", true,
				Helper.UTF8NoBom.GetBytes("{}"), new byte[0]);
		}

		[Fact]
		public void can_be_created() {
			var t = new StreamPositionTagger(0, "stream1");
			new PositionTracker(t);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_after_case() {
			var t = new StreamPositionTagger(0, "stream1");
			var result = t.IsMessageAfterCheckpointTag(CheckpointTag.FromStreamPosition(0, "stream1", 0), _firstEvent);
			Assert.True(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_before_case() {
			var t = new StreamPositionTagger(0, "stream1");
			var result = t.IsMessageAfterCheckpointTag(CheckpointTag.FromStreamPosition(0, "stream1", 2), _firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_equal_case() {
			var t = new StreamPositionTagger(0, "stream1");
			var result = t.IsMessageAfterCheckpointTag(CheckpointTag.FromStreamPosition(0, "stream1", 1), _firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_incompatible_case() {
			// events from other streams are not after any tag
			var t = new StreamPositionTagger(0, "stream-other");
			var result = t.IsMessageAfterCheckpointTag(CheckpointTag.FromStreamPosition(0, "stream1", 1), _firstEvent);
			Assert.False(result);
		}


		[Fact]
		public void null_stream_throws_argument_null_exception() {
			Assert.Throws<ArgumentNullException>(() => { new StreamPositionTagger(0, null); });
		}

		[Fact]
		public void empty_stream_throws_argument_exception() {
			Assert.Throws<ArgumentException>(() => { new StreamPositionTagger(0, ""); });
		}

		[Fact]
		public void position_checkpoint_tag_is_incompatible() {
			var t = new StreamPositionTagger(0, "stream1");
			Assert.False(t.IsCompatible(CheckpointTag.FromPosition(0, 1000, 500)));
		}

		[Fact]
		public void anothe_stream_checkpoint_tag_is_incompatible() {
			var t = new StreamPositionTagger(0, "stream1");
			Assert.False(t.IsCompatible(CheckpointTag.FromStreamPosition(0, "stream2", 100)));
		}

		[Fact]
		public void the_same_stream_checkpoint_tag_is_compatible() {
			var t = new StreamPositionTagger(0, "stream1");
			Assert.True(t.IsCompatible(CheckpointTag.FromStreamPosition(0, "stream1", 100)));
		}

		[Fact]
		public void adjust_compatible_tag_returns_the_same_tag() {
			var t = new StreamPositionTagger(0, "stream1");
			var tag = CheckpointTag.FromStreamPosition(0, "stream1", 1);
			Assert.Equal(tag, t.AdjustTag(tag));
		}

		[Fact]
		public void can_adjust_multi_stream_position_tag() {
			var t = new StreamPositionTagger(0, "stream1");
			var tag = CheckpointTag.FromStreamPosition(0, "stream1", 1);
			var original =
				CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 1}, {"stream2", 2}});
			Assert.Equal(tag, t.AdjustTag(original));
		}

		[Fact]
		public void zero_position_tag_is_before_first_event_possible() {
			var t = new StreamPositionTagger(0, "stream1");
			var zero = t.MakeZeroCheckpointTag();

			var zeroFromEvent = t.MakeCheckpointTag(zero, _zeroEvent);

			Assert.True(zeroFromEvent > zero);
		}

		[Fact]
		public void produced_checkpoint_tags_are_correctly_ordered() {
			var t = new StreamPositionTagger(0, "stream1");
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
