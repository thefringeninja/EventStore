using System;
using System.Collections.Generic;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public abstract class TestFixtureWithJsProjection : IDisposable {
		private ProjectionStateHandlerFactory _stateHandlerFactory;
		protected IProjectionStateHandler _stateHandler;
		protected List<string> _logged;
		protected string _projection;
		protected string _state = null;
		protected string _sharedState = null;
		protected IQuerySources _source;

		public TestFixtureWithJsProjection() {
			Setup();
		}

		public void Setup() {
			_state = null;
			_projection = null;
			Given();
			_logged = new List<string>();
			_stateHandlerFactory = new ProjectionStateHandlerFactory();
			_stateHandler = _stateHandlerFactory.Create(
				"JS", _projection, logger: (s, _) => {
					if (s.StartsWith("P:"))
						Console.WriteLine(s);
					else
						_logged.Add(s);
				}); // skip prelude debug output
			_source = _stateHandler.GetSourceDefinition();

			if (_state != null)
				_stateHandler.Load(_state);
			else
				_stateHandler.Initialize();

			if (_sharedState != null)
				_stateHandler.LoadShared(_sharedState);
			When();
		}

		protected virtual void When() {
		}

		protected abstract void Given();

		public void Dispose() => Teardown();

		protected void Teardown() {
			_stateHandler?.Dispose();
			_stateHandler = null;
			GC.Collect(2, GCCollectionMode.Forced);
			GC.WaitForPendingFinalizers();
		}
	}
}
