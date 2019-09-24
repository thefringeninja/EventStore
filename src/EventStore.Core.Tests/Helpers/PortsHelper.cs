using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using EventStore.Common.Log;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.NetworkInformation;

namespace EventStore.Core.Tests.Helpers {
	public static class PortsHelper {
		private static readonly ILogger Log = LogManager.GetLogger("PortsHelper");

		public static int GetAvailablePort(IPAddress ip) {
			using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
			socket.Bind(new IPEndPoint(ip, 0));
			var availablePort = ((IPEndPoint)socket.LocalEndPoint).Port;

			Log.Trace($"Find port {availablePort} for use.");

			return availablePort;
		}
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
