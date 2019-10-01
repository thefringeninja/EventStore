using System.Diagnostics;
using Xunit;

namespace EventStore.Core.Tests {
	public class DebugFactAttribute : FactAttribute {
		public DebugFactAttribute() {
			SkipIfDebug();
		}

		[Conditional("DEBUG")]
		private void SkipIfDebug() {
			Skip = "These tests require DEBUG conditional";
		}
	}
	public class ExplicitAttribute : FactAttribute {
		public ExplicitAttribute() {
			if (!Debugger.IsAttached) {
				Skip = "Only running in interactive mode.";
			}
		}
	}

	public class ExplicitTheoryAttribute : TheoryAttribute {
		public ExplicitTheoryAttribute() {
			if (!Debugger.IsAttached) {
				Skip = "Only running in interactive mode.";
			}
		}		
	}
}
