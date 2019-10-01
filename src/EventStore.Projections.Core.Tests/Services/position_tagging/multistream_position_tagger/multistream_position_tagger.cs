using System;
using System.Collections.Generic;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Core.Tests.Helpers;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.multistream_position_tagger {
	public class multistream_position_tagger {
		private ReaderSubscriptionMessage.CommittedEventDistributed _zeroEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _firstEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _secondEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _thirdEvent;

		public multistream_position_tagger() {
			_zeroEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(10, 0), "stream1", 0, false, Guid.NewGuid(), "StreamCreated", false,
				new byte[0], new byte[0]);
			_firstEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(30, 20), "stream1", 1, false, Guid.NewGuid(), "Data", true,
				Helper.UTF8NoBom.GetBytes("{}"), new byte[0]);
			_secondEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(50, 40), "stream2", 0, false, Guid.NewGuid(), "StreamCreated", false,
				new byte[0], new byte[0]);
			_thirdEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(70, 60), "stream2", 1, false, Guid.NewGuid(), "Data", true,
				Helper.UTF8NoBom.GetBytes("{}"), new byte[0]);
		}

		[Fact]
		public void can_be_created() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			new PositionTracker(t);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_after_case() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 0}, {"stream2", 0}}),
					_firstEvent);
			Assert.True(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_before_case() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 2}, {"stream2", 2}}),
					_firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_equal_case() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromStreamPositions(0, new Dictionary<string, long> {{"stream1", 1}, {"stream2", 1}}),
					_firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_incompatible_streams_case() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream-other", "stream2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromStreamPositions(0,
						new Dictionary<string, long> {{"stream-other", 0}, {"stream2", 0}}),
					_firstEvent);
			Assert.False(result);
		}


		[Fact]
		public void null_streams_throws_argument_null_exception() {
			Assert.Throws<ArgumentNullException>(() => { new MultiStreamPositionTagger(0, null); });
		}

		[Fact]
		public void empty_streams_throws_argument_exception() {
			Assert.Throws<ArgumentException>(() => { new MultiStreamPositionTagger(0, new string[] { }); });
		}

		[Fact]
		public void position_checkpoint_tag_is_incompatible() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			Assert.False(t.IsCompatible(CheckpointTag.FromPosition(0, 1000, 500)));
		}

		[Fact]
		public void another_streams_checkpoint_tag_is_incompatible() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			Assert.False(
				t.IsCompatible(
					CheckpointTag.FromStreamPositions(0,
						new Dictionary<string, long> {{"stream2", 100}, {"stream3", 150}})));
		}

		[Fact]
		public void the_same_stream_checkpoint_tag_is_compatible() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			Assert.True(
				t.IsCompatible(
					CheckpointTag.FromStreamPositions(0,
						new Dictionary<string, long> {{"stream1", 100}, {"stream2", 150}})));
		}

		[Fact]
		public void adjust_compatible_tag_returns_the_same_tag() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			var tag = CheckpointTag.FromStreamPositions(0,
				new Dictionary<string, long> {{"stream1", 1}, {"stream2", 2}});
			Assert.Equal(tag, t.AdjustTag(tag));
		}

		[Fact]
		public void can_adjust_stream_position_tag() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			var tag = CheckpointTag.FromStreamPositions(0,
				new Dictionary<string, long> {{"stream1", 1}, {"stream2", -1}});
			var original = CheckpointTag.FromStreamPosition(0, "stream1", 1);
			Assert.Equal(tag, t.AdjustTag(original));
		}

		[Fact]
		public void zero_position_tag_is_before_first_event_possible() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			var zero = t.MakeZeroCheckpointTag();

			var zeroFromEvent = t.MakeCheckpointTag(zero, _zeroEvent);

			Assert.True(zeroFromEvent > zero);
		}

		[Fact]
		public void produced_checkpoint_tags_are_correctly_ordered() {
			var t = new MultiStreamPositionTagger(0, new[] {"stream1", "stream2"});
			var zero = t.MakeZeroCheckpointTag();

			var zeroEvent = t.MakeCheckpointTag(zero, _zeroEvent);
			var zeroEvent2 = t.MakeCheckpointTag(zeroEvent, _zeroEvent);
			var first = t.MakeCheckpointTag(zeroEvent2, _firstEvent);
			var second = t.MakeCheckpointTag(first, _secondEvent);
			var second2 = t.MakeCheckpointTag(zeroEvent, _secondEvent);
			var third = t.MakeCheckpointTag(second, _thirdEvent);

			Assert.True(zeroEvent > zero);
			Assert.True(first > zero);
			Assert.True(second > first);

			Assert.Equal(zeroEvent2, zeroEvent);
			Assert.NotEqual(second, second2);
			Assert.True(second2 > zeroEvent);
			Assert.Throws<InvalidOperationException>(() => TestHelper.Consume(second2 > first));

			Assert.True(third > second);
			Assert.True(third > first);
			Assert.True(third > zeroEvent);
			Assert.True(third > zero);
		}
	}
}
