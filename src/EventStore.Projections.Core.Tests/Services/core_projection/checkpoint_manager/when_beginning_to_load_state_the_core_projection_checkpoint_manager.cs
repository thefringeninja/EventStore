using System;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.checkpoint_manager {
	public class when_beginning_to_load_state_the_core_projection_checkpoint_manager :
		TestFixtureWithCoreProjectionCheckpointManager {
		private Exception _exception;

		protected override void When() {
			base.When();
			_exception = null;
			try {
				_checkpointReader.BeginLoadState();
			} catch (Exception ex) {
				_exception = ex;
			}
		}

		[Fact]
		public void it_can_be_invoked() {
			Assert.Null(_exception);
		}

		[Fact]
		public void start_throws_invalid_operation_exception() {
			Assert.Throws<InvalidOperationException>(() => { _checkpointReader.BeginLoadState(); });
		}

		[Fact]
		public void stopping_throws_invalid_operation_exception() {
			Assert.Throws<InvalidOperationException>(() => { _manager.Stopping(); });
		}

		[Fact]
		public void stopped_throws_invalid_operation_exception() {
			Assert.Throws<InvalidOperationException>(() => { _manager.Stopped(); });
		}

		[Fact]
		public void event_processed_throws_invalid_operation_exception() {
//            _manager.StateUpdated("", @"{""state"":""state""}");
			Assert.Throws<InvalidOperationException>(() => {
				_manager.EventProcessed(CheckpointTag.FromStreamPosition(0, "stream", 10), 77.7f);
			});
		}

		[Fact]
		public void checkpoint_suggested_throws_invalid_operation_exception() {
			Assert.Throws<InvalidOperationException>(() => {
				_manager.CheckpointSuggested(CheckpointTag.FromStreamPosition(0, "stream", 10), 77.7f);
			});
		}

		[Fact]
		public void ready_for_checkpoint_throws_invalid_operation_exception() {
			Assert.Throws<InvalidOperationException>(() => {
				_manager.Handle(new CoreProjectionProcessingMessage.ReadyForCheckpoint(null));
			});
		}

		[Fact]
		public void can_be_started() {
			_manager.Start(CheckpointTag.FromStreamPosition(0, "stream", 10), null);
		}

		[Fact]
		public void cannot_be_started_from_incompatible_checkpoint_tag() {
			//TODO: move to when loaded
			Assert.Throws<InvalidOperationException>(() => {
				_manager.Start(CheckpointTag.FromStreamPosition(0, "stream1", 10), null);
			});
		}
	}
}
