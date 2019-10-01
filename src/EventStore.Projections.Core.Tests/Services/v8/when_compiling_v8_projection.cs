using System;
using System.Collections.Generic;
using EventStore.Core.Tests;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.v8;
using EventStore.Projections.Core.v8;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.v8 {
	public class when_compiling_v8_projection {
		private List<string> _logged;
		private ProjectionStateHandlerFactory _stateHandlerFactory;
		private IProjectionStateHandler _stateHandler;
		private string _projection;
		private Action<string, object[]> _logger;
		private readonly Js1.LogDelegate _logDelegate = Console.WriteLine;
		private Js1.LoadModuleDelegate _loadModuleDelegate;

		[Explicit, Trait("Category", "v8"), Trait("Category", "Manual")]
		public void can_compile_million_times() {
			for (var i = 0; i < 10000000; i++) {
				if (_stateHandler != null)
					_stateHandler.Dispose();
				_stateHandler = null;
/*
                _state = null;
*/
				_projection = null;
				_projection = @"
                fromAll();
                on_raw(function(state, event, streamId, eventType, sequenceNumber, metadata) {
                    emit('output-stream' + sequenceNumber, 'emitted-event' + sequenceNumber, {a: JSON.parse(event).a});
                    return {};
                });
            ";
				_logged = new List<string>();
				_stateHandlerFactory = new ProjectionStateHandlerFactory();
				_stateHandler = _stateHandlerFactory.Create(
					"JS", _projection, logger: (s, _) => {
						if (!s.StartsWith("P:")) _logged.Add(s);
						else _logDelegate(s);
					}); // skip prelude debug output
/*
                if (_state != null)
                    _stateHandler.Load(_state);
                else
                    _stateHandler.Initialize();
*/
				Console.Write(".");
			}
		}

		[Explicit, Trait("Category", "v8"), Trait("Category", "Manual")]
		public void can_compile_prelude_million_times() {
			_logger = (s, _) => {
				if (!s.StartsWith("P:")) _logged.Add(s);
				else _logDelegate(s);
			};
			_projection = @"
                fromAll();
                on_raw(function(state, event, streamId, eventType, sequenceNumber, metadata) {
                    emit('output-stream' + sequenceNumber, 'emitted-event' + sequenceNumber, {a: JSON.parse(event).a});
                    return {};
                });
            ";
			for (var i = 0; i < 10000000; i++) {
				_logged = new List<string>();
				var preludeSource = DefaultV8ProjectionStateHandler.GetModuleSource("1Prelude");
				using (
					new PreludeScript(
						preludeSource.Item1, preludeSource.Item2, DefaultV8ProjectionStateHandler.GetModuleSource,
						(i1, action) => { },
						_logger)) {
				}
			}
		}

		[Explicit, Trait("Category", "v8"), Trait("Category", "Manual")]
		public void can_compile_script_million_times() {
			_loadModuleDelegate = (ptr, name) => IntPtr.Zero;
			for (var i = 0; i < 10000000; i++) {
				IntPtr prelude = Js1.CompilePrelude("return {};", "test.js", _loadModuleDelegate, () => true,
					() => true, _logDelegate);
				Js1.DisposeScript(prelude);
			}
		}
	}
}
