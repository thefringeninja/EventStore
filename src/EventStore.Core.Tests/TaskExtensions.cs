using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Xunit;

namespace EventStore.Core.Tests {
	public static class TaskExtensions {
		public static Task WithTimeout(this Task task, TimeSpan timeout)
			=> task.WithTimeout(Convert.ToInt32(timeout.TotalMilliseconds));

		public static async Task WithTimeout(this Task task, int timeoutMs = 3000) {
			if (Debugger.IsAttached) {
				timeoutMs = -1;
			}

			if (await Task.WhenAny(task, Task.Delay(timeoutMs)) != task)
				throw new TimeoutException("Timed out waiting for task");
			await task;
		}

		public static Task<T> WithTimeout<T>(this Task<T> task, TimeSpan timeout)
			=> task.WithTimeout(Convert.ToInt32(timeout.TotalMilliseconds));

		public static async Task<T> WithTimeout<T>(this Task<T> task, int timeoutMs = 3000) {
			if (Debugger.IsAttached) {
				timeoutMs = -1;
			}

			if (await Task.WhenAny(task, Task.Delay(timeoutMs)) == task)
				return await task;
			throw new TimeoutException("Timed out waiting for task");
		}
	}

	[AttributeUsage(AttributeTargets.Method)]
	public class PlatformFactAttribute : FactAttribute {
		public PlatformFactAttribute(string platform) {
			if (!RuntimeInformation.IsOSPlatform(OSPlatform.Create(platform))) {
				Skip = $"Test only valid for platform {platform}";
			}
		}
	}
}
