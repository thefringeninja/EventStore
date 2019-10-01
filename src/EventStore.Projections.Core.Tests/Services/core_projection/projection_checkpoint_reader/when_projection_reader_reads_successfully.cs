using System;
using System.Threading;
using EventStore.Core.Bus;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.core_projection.projection_checkpoint_reader {
	public class when_projection_reader_reads_successfully : with_projection_checkpoint_reader,
		IHandle<CoreProjectionProcessingMessage.CheckpointLoaded> {
		private ManualResetEventSlim _mre = new ManualResetEventSlim();
		private CoreProjectionProcessingMessage.CheckpointLoaded _checkpointLoaded;

		public override void When() {
			_bus.Subscribe<CoreProjectionProcessingMessage.CheckpointLoaded>(this);

			_reader.Initialize();
			_reader.BeginLoadState();
			if (!_mre.Wait(10000)) {
				throw new Exception("Timed out waiting for checkpoint to load");
			}
		}

		public void Handle(CoreProjectionProcessingMessage.CheckpointLoaded message) {
			_checkpointLoaded = message;
			_mre.Set();
		}

		[Fact]
		public void should_load_checkpoint() {
			Assert.NotNull(_checkpointLoaded);
			Assert.Equal(_checkpointLoaded.ProjectionId, _projectionId);
		}
	}
}
