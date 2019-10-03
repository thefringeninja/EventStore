using System;
using System.Collections.Generic;
using System.Net;
using EventStore.ClientAPI.SystemData;

namespace EventStore.ClientAPI.Transport.Tcp {
	internal interface ITcpConnection {
		Guid ConnectionId { get; }
		IPEndPoint RemoteEndPoint { get; }
		IPEndPoint LocalEndPoint { get; }
		int SendQueueSize { get; }
		bool IsClosed { get; }

		void ReceiveAsync(Action<ITcpConnection, IEnumerable<ArraySegment<byte>>> callback);
		void EnqueueSend(TcpPackage package);
		void Close(string reason);
		void StartReceiving();
	}
}
