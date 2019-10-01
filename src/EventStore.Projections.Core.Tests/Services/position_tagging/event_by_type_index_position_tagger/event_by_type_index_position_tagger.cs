using System;
using System.Collections.Generic;
using System.Text;
using EventStore.Common.Utils;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.position_tagging.event_by_type_index_position_tagger {
	public class event_by_type_index_position_tagger {
		private ReaderSubscriptionMessage.CommittedEventDistributed _zeroEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _firstEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _secondEvent;
		private ReaderSubscriptionMessage.CommittedEventDistributed _thirdEvent;

		public event_by_type_index_position_tagger() {
			_zeroEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(-1, 120), new TFPos(20, 10), "$et-type1", 0, "stream1", 0, true,
				Guid.NewGuid(), "type1", true, Helper.UTF8NoBom.GetBytes("{}"), new byte[0], null, 10f);

			_firstEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(-1, 130), new TFPos(30, 20), "$et-type2", 0, "stream1", 1, true,
				Guid.NewGuid(), "type2", true, Helper.UTF8NoBom.GetBytes("{}"), new byte[0], null, 20f);

			_secondEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(-1, 140), new TFPos(50, 40), "$et-type1", 1, "stream2", 0, false,
				Guid.NewGuid(), "type1", true, Helper.UTF8NoBom.GetBytes("{}"), new byte[0], null, 30f);

			_thirdEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(-1, 150), new TFPos(70, 60), "$et-type2", 1, "stream2", 1, false,
				Guid.NewGuid(), "type2", true, Helper.UTF8NoBom.GetBytes("{}"), new byte[0], null, 40f);
		}

		[Fact]
		public void can_be_created() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			new PositionTracker(t);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_after_case() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(10, 5),
						new Dictionary<string, long> {{"type1", 0}, {"type2", -1}}), _firstEvent);
			Assert.True(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_tf_only_after_case() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(10, 5),
						new Dictionary<string, long> {{"type1", 0}, {"type2", 0}}), _firstEvent);
			Assert.True(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_before_case() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(40, 35),
						new Dictionary<string, long> {{"type1", 2}, {"type2", 2}}),
					_firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_tf_only_before_case() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(40, 35),
						new Dictionary<string, long> {{"type1", 0}, {"type2", 0}}),
					_firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_equal_case() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(30, 20),
						new Dictionary<string, long> {{"type1", 0}, {"type2", 0}}),
					_firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_tf_only_equal_case() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(30, 20),
						new Dictionary<string, long> {{"type1", -1}, {"type2", -1}}),
					_firstEvent);
			Assert.False(result);
		}

		[Fact]
		public void is_message_after_checkpoint_tag_incompatible_streams_case() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var result =
				t.IsMessageAfterCheckpointTag(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(30, 20),
						new Dictionary<string, long> {{"type1", -1}, {"type3", -1}}),
					_firstEvent);
			Assert.False(result);
		}


		[Fact]
		public void null_streams_throws_argument_null_exception() {
			Assert.Throws<ArgumentNullException>(() => { new EventByTypeIndexPositionTagger(0, null); });
		}

		[Fact]
		public void empty_streams_throws_argument_exception() {
			Assert.Throws<ArgumentException>(() => { new EventByTypeIndexPositionTagger(0, new string[] { }); });
		}

		[Fact]
		public void position_checkpoint_tag_is_incompatible() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			Assert.False(t.IsCompatible(CheckpointTag.FromPosition(0, 1000, 500)));
		}

		[Fact]
		public void streams_checkpoint_tag_is_incompatible() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			Assert.False(
				t.IsCompatible(
					CheckpointTag.FromStreamPositions(0,
						new Dictionary<string, long> {{"$et-type1", 100}, {"$et-type2", 150}})));
		}

		[Fact]
		public void another_events_checkpoint_tag_is_compatible() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			Assert.False(
				t.IsCompatible(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(100, 50),
						new Dictionary<string, long> {{"type1", 100}, {"type3", 150}})));
		}

		[Fact]
		public void the_same_events_checkpoint_tag_is_compatible() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			Assert.True(
				t.IsCompatible(
					CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(100, 50),
						new Dictionary<string, long> {{"type1", 100}, {"type2", 150}})));
		}

		[Fact]
		public void adjust_compatible_tag_returns_the_same_tag() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var tag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(100, 50),
				new Dictionary<string, long> {{"type1", 1}, {"type2", 2}});
			Assert.Equal(tag, t.AdjustTag(tag));
		}

		[Fact]
		public void can_adjust_tf_position_tag() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var tag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(100, 50),
				new Dictionary<string, long> {{"type1", 1}, {"type2", 2}});
			var original = CheckpointTag.FromPosition(0, 100, 50);
			Assert.Equal(tag, t.AdjustTag(original));
		}

		[Fact]
		public void zero_position_tag_is_before_first_event_possible() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var zero = t.MakeZeroCheckpointTag();

			var zeroFromEvent = t.MakeCheckpointTag(zero, _zeroEvent);

			Assert.True(zeroFromEvent > zero);
		}

		[Fact]
		public void can_update_by_tf_event_if_with_prior_index_position() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var linkEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(180, 170), "$et-type2", 1, false, Guid.NewGuid(), "$>", false,
				Helper.UTF8NoBom.GetBytes("0@stream2"), new byte[0]);
			var tag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(70, 60),
				new Dictionary<string, long> {{"type1", 2}, {"type2", 2}});
			var updated = t.MakeCheckpointTag(tag, linkEvent);
			Assert.Equal(new TFPos(180, 170), updated.Position);
			Assert.Equal(2, updated.Streams["type1"]);
			Assert.Equal(2, updated.Streams["type2"]);
		}

		[Fact]
		public void cannot_update_by_prior_tf_position() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
			var linkEvent = ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				Guid.NewGuid(), new TFPos(180, 170), "$et-type2", 1, false, Guid.NewGuid(), "$>", false,
				Helper.UTF8NoBom.GetBytes("0@stream2"), new byte[0]);
			var tag = CheckpointTag.FromEventTypeIndexPositions(0, new TFPos(270, 260),
				new Dictionary<string, long> {{"type1", 2}, {"type2", 2}});
			Assert.Throws<InvalidOperationException>(() => { t.MakeCheckpointTag(tag, linkEvent); });
		}

		[Fact]
		public void produced_checkpoint_tags_are_correctly_ordered() {
			var t = new EventByTypeIndexPositionTagger(0, new[] {"type1", "type2"});
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
			Assert.Equal(second, second2); // strong order (by tf)
			Assert.True(second2 > zeroEvent);
			Assert.True(second2 > first);

			Assert.True(third > second);
			Assert.True(third > first);
			Assert.True(third > zeroEvent);
			Assert.True(third > zero);
		}
	}
}
