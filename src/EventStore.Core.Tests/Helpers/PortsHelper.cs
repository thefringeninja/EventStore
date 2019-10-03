using System;
using System.Linq;
using System.Net;
using EventStore.Common.Log;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.NetworkInformation;

namespace EventStore.Core.Tests.Helpers {
	public static class PortsHelper {
		private static readonly ILogger Log = LogManager.GetLogger("PortsHelper");

		public const int PortStart = 45000;
		public const int PortCount = 200;

		private static readonly ConcurrentQueue<int> AvailablePorts =
			new ConcurrentQueue<int>(Enumerable.Range(PortStart, PortCount));


		public static int GetAvailablePort(IPAddress ip) {
			const int maxAttempts = 50;
			var properties = IPGlobalProperties.GetIPGlobalProperties();

			var inUse = new HashSet<int>(properties.GetActiveTcpConnections().Select(x => x.LocalEndPoint.Port)
				.Concat(properties.GetActiveTcpListeners().Select(x => x.Port))
				.Concat(properties.GetActiveUdpListeners().Select(x => x.Port)));

			var attempt = 0;

			while (attempt++ < maxAttempts && AvailablePorts.TryDequeue(out var port)) {
				if (!inUse.Contains(port)) {
					return port;
				}

				AvailablePorts.Enqueue(port);
			}

			throw new Exception($"Could not find free port after {maxAttempts} attempts.");
		}

		public static void ReturnPort(int port) => AvailablePorts.Enqueue(port);
/*
        private static int[] GetRandomPorts(int from, int portCount)
        {
            var res = new int[portCount];
            var rnd = new Random(Math.Abs(Guid.NewGuid().GetHashCode()));
            for (int i = 0; i < portCount; ++i)
            {
                res[i] = from + i;
            }
            for (int i = 0; i < portCount; ++i)
            {
                int index = rnd.Next(portCount - i);
                int tmp = res[i];
                res[i] = res[i + index];
                res[i + index] = tmp;
            }
            return res;
        }
*/
	}
}
