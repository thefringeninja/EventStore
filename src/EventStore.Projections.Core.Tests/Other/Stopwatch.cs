using System;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Projections.Core.Tests.Other {
	public class Stopwatch {
		[Fact]
		public void MeasureStopwatch() {
			var sw = new System.Diagnostics.Stopwatch();
			var measured = new System.Diagnostics.Stopwatch();
			sw.Reset();
			sw.Start();
			measured.Start();
			measured.Stop();
			TestHelper.Consume(measured.ElapsedMilliseconds);
			sw.Stop();
			TestHelper.Consume(sw.ElapsedMilliseconds);
			measured.Reset();
			sw.Reset();

			sw.Start();
			sw.Stop();
			var originalTime = sw.ElapsedMilliseconds;
			sw.Reset();

			sw.Start();
			for (var i = 0; i < 1000000; i++) {
				measured.Start();
				measured.Stop();
				TestHelper.Consume(measured.ElapsedMilliseconds);
			}

			sw.Stop();
			var measuredTime = sw.ElapsedMilliseconds;
			Console.WriteLine(measuredTime - originalTime);
		}
	}
}
