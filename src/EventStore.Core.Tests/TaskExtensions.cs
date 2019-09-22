using System;
using System.Threading.Tasks;

namespace EventStore.Core.Tests {
	internal static class TaskExtensions {
		public static Task WithTimeout(this Task task, int timeoutMs = 3000)
			=> task.WithTimeout(TimeSpan.FromMilliseconds(timeoutMs));

		public static async Task WithTimeout(this Task task, TimeSpan timeout) {
			if(await Task.WhenAny(task, Task.Delay(timeout)) != task)
				throw new TimeoutException("Timed out waiting for task");
			await task;
		}

		public static Task<T> WithTimeout<T>(this Task<T> task, int timeoutMs = 3000)
			=> task.WithTimeout(TimeSpan.FromMilliseconds(timeoutMs));

		public static async Task<T> WithTimeout<T>(this Task<T> task, TimeSpan timeout) {
			if (await Task.WhenAny(task, Task.Delay(timeout)) == task)
				return await task;
			throw new TimeoutException("Timed out waiting for task");
		}
	}
}
