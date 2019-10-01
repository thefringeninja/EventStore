using System;
using System.Threading;
using EventStore.Core.Tests;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.v8;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.v8 {
	public class when_creating_v8_projection {
		private ProjectionStateHandlerFactory _stateHandlerFactory;

		public when_creating_v8_projection() {
			_stateHandlerFactory = new ProjectionStateHandlerFactory();
		}

		[Fact, Trait("Category", "v8")]
		public void api_can_be_used() {
			var ver = Js1.ApiVersion();
			Console.WriteLine(ver);
		}

		[Fact, Trait("Category", "v8")]
		public void api_can_be_used2() {
			var ver = Js1.ApiVersion();
			Console.WriteLine(ver);
		}

		[Fact, Trait("Category", "v8")]
		public void it_can_be_created() {
			using (_stateHandlerFactory.Create("JS", @"")) {
			}
		}

		[Fact, Trait("Category", "v8")]
		public void it_can_log_messages() {
			string m = null;
			using (_stateHandlerFactory.Create("JS", @"log(""Message1"");", logger: (s, _) => m = s)) {
			}

			Assert.Equal("Message1", m);
		}

		[Fact, Trait("Category", "v8")]
		public void js_syntax_errors_are_reported() {
			try {
				using (_stateHandlerFactory.Create("JS", @"log(1;", logger: (s, _) => { })) {
				}
			} catch (Exception ex) {
				Assert.IsType<Js1Exception>(ex);
				Assert.True(ex.Message.StartsWith("SyntaxError:"));
			}
		}

		[Fact, Trait("Category", "v8")]
		public void js_exceptions_errors_are_reported() {
			try {
				using (_stateHandlerFactory.Create("JS", @"throw 123;", logger: (s, _) => { })) {
				}
			} catch (Exception ex) {
				Assert.IsType<Js1Exception>(ex);
				Assert.Equal("123", ex.Message);
			}
		}

		[Fact, Trait("Category", "v8")]
		public void long_compilation_times_out() {
			try {
				using (_stateHandlerFactory.Create("JS",
					@"
                                var i = 0;
                                while (true) i++;
                    ",
					logger: (s, _) => { },
					cancelCallbackFactory: (timeout, action) => ThreadPool.QueueUserWorkItem(state => {
						Console.WriteLine("Calling a callback in " + timeout + "ms");
						Thread.Sleep(timeout);
						action();
					}))) {
				}
			} catch (Exception ex) {
				Assert.IsType<Js1Exception>(ex);
				Assert.True(ex.Message.Contains("terminated"));
			}
		}

		[Fact, Trait("Category", "v8")]
		public void long_execution_times_out() {
			try {
				//string m = null;
				using (var h = _stateHandlerFactory.Create("JS",
					@"
                        fromAll().when({
                            $any: function (s, e) {
                                log('1');
                                var i = 0;
                                while (true) i++;
                            }
                        });
                    ",
					logger: Console.WriteLine,
					cancelCallbackFactory: (timeout, action) => ThreadPool.QueueUserWorkItem(state => {
						Console.WriteLine("Calling a callback in " + timeout + "ms");
						Thread.Sleep(timeout);
						action();
					}))) {
					h.Initialize();
					string newState;
					EmittedEventEnvelope[] emittedevents;
					h.ProcessEvent(
						"partition", CheckpointTag.FromPosition(0, 100, 50), "stream", "event", "", Guid.NewGuid(), 1,
						"", "{}",
						out newState, out emittedevents);
				}
			} catch (Exception ex) {
				Assert.IsType<Js1Exception>(ex);
				Assert.True(ex.Message.Contains("terminated"));
			}
		}

		[Fact, Trait("Category", "v8")]
		public void long_post_processing_times_out() {
			try {
				//string m = null;
				using (var h = _stateHandlerFactory.Create("JS",
					@"
                        fromAll().when({
                            $any: function (s, e) {
                                return {};
                            }
                        })
                        .transformBy(function(s){
                                log('1');
                                var i = 0;
                                while (true) i++;
                        });
                    ",
					logger: Console.WriteLine,
					cancelCallbackFactory: (timeout, action) => ThreadPool.QueueUserWorkItem(state => {
						Console.WriteLine("Calling a callback in " + timeout + "ms");
						Thread.Sleep(timeout);
						action();
					}))) {
					h.Initialize();
					string newState;
					EmittedEventEnvelope[] emittedevents;
					h.ProcessEvent(
						"partition", CheckpointTag.FromPosition(0, 100, 50), "stream", "event", "", Guid.NewGuid(), 1,
						"", "{}",
						out newState, out emittedevents);
					h.TransformStateToResult();
				}
			} catch (Exception ex) {
				Assert.IsType<Js1Exception>(ex);
				Assert.True(ex.Message.Contains("terminated"));
			}
		}

		[Explicit, Trait("Category", "v8"), Trait("Category", "Manual")]
		public void long_execution_times_out_many() {
			//string m = null;
			for (var i = 0; i < 10; i++) {
				Console.WriteLine(i);
				try {
					using (var h = _stateHandlerFactory.Create(
						"JS", @"
                    fromAll().when({
                        $any: function (s, e) {
                            log('1');
                            var i = 0;
                            while (true) i++;
                        }
                    });
                ", logger: Console.WriteLine,
						cancelCallbackFactory: (timeout, action) => ThreadPool.QueueUserWorkItem(
							state => {
								Console.WriteLine("Calling a callback in " + timeout + "ms");
								Thread.Sleep(timeout);
								action();
							}))) {
						h.Initialize();
						string newState;
						EmittedEventEnvelope[] emittedevents;
						h.ProcessEvent(
							"partition", CheckpointTag.FromPosition(0, 100, 50), "stream", "event", "", Guid.NewGuid(),
							1,
							"", "{}", out newState, out emittedevents);
					}
				} catch (Js1Exception) {
				}
			}
		}
	}
}
