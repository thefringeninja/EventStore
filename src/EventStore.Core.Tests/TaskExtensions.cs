using System;
using System.Threading.Tasks;

namespace EventStore.Core.Tests {
	internal static class TaskExtensions {
		public static async Task WithTimeout(this Task task, int timeoutMs = 3000) {
			var timeoutTask = Task.Delay(timeoutMs);

			if (timeoutTask == await Task.WhenAny(task, timeoutTask)) {
				throw new TimeoutException();
			}

			await task;
		}

		public static async Task<T> WithTimeout<T>(this Task<T> task, int timeoutMs = 3000) {
			var timeoutTask = Task.Delay(timeoutMs);

			if (timeoutTask == await Task.WhenAny(task, timeoutTask)) {
				throw new TimeoutException();
			}

			return await task;
		}
	}
}
