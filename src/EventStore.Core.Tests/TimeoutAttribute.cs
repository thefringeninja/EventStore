using System;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace EventStore.Core.Tests {
	[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class | AttributeTargets.Assembly, Inherited = false)]
	public class TimeoutAttribute : PropertyAttribute, IApplyToContext {
		private readonly int _timeout;

		/// <summary>
		/// Construct a TimeoutAttribute given a time in milliseconds
		/// </summary>
		/// <param name="timeout">The timeout value in milliseconds</param>
		public TimeoutAttribute(int timeout)
			: base(timeout) {
			_timeout = timeout;
		}

		public void ApplyToContext(TestExecutionContext context) {
			context.TestCaseTimeout = _timeout;
		}
	}
}
