using System;
using System.Diagnostics;

namespace EventStore.Core
{
	internal static class ESDebug {
		public static void Assert(bool condition, string message = null) {
			if (condition) {
				return;
			}

			if (Debugger.IsAttached) {
				if (message == null) {
					Debug.Assert(condition);
				} else {
					Debug.Assert(condition, message);
				}
			} else {
				throw new Exception($"Assertion failed: {message}");
			}
		}

		public static void WriteLine(string message, params object[] args) =>
			Debug.WriteLine(message, args);

		public static void WriteLine(Exception ex) =>
			Debug.WriteLine(ex);
	}
}
