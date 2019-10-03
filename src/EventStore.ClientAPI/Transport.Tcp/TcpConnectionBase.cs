using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using EventStore.ClientAPI.Common.Utils;
using EventStore.ClientAPI.SystemData;

namespace EventStore.ClientAPI.Transport.Tcp {
	internal abstract class TcpConnectionBase : IMonitoredTcpConnection, ITcpConnection {
		public abstract Guid ConnectionId { get; }
		public IPEndPoint RemoteEndPoint {
			get { return _remoteEndPoint; }
		}

		public IPEndPoint LocalEndPoint {
			get { return _localEndPoint; }
		}

		public abstract int SendQueueSize { get; }

		public bool IsInitialized {
			get { return _socket != null; }
		}

		public bool IsClosed {
			get { return _isClosed; }
		}

		public bool InSend {
			get { return Interlocked.Read(ref _lastSendStarted) >= 0; }
		}

		public bool InReceive {
			get { return Interlocked.Read(ref _lastReceiveStarted) >= 0; }
		}

		public int PendingSendBytes {
			get { return _pendingSendBytes; }
		}

		public int InSendBytes {
			get { return _inSendBytes; }
		}

		public int PendingReceivedBytes {
			get { return _pendingReceivedBytes; }
		}

		public long TotalBytesSent {
			get { return Interlocked.Read(ref _totalBytesSent); }
		}

		public long TotalBytesReceived {
			get { return Interlocked.Read(ref _totalBytesReceived); }
		}

		public int SendCalls {
			get { return _sentAsyncs; }
		}

		public int SendCallbacks {
			get { return _sentAsyncCallbacks; }
		}

		public int ReceiveCalls {
			get { return _recvAsyncs; }
		}

		public int ReceiveCallbacks {
			get { return _recvAsyncCallbacks; }
		}

		public bool IsReadyForSend {
			get {
				try {
					return !_isClosed && _socket.Poll(0, SelectMode.SelectWrite);
				} catch (ObjectDisposedException) {
					//TODO: why do we get this?
					return false;
				}
			}
		}

		public bool IsReadyForReceive {
			get {
				try {
					return !_isClosed && _socket.Poll(0, SelectMode.SelectRead);
				} catch (ObjectDisposedException) {
					//TODO: why do we get this?
					return false;
				}
			}
		}

		public bool IsFaulted {
			get {
				try {
					return !_isClosed && _socket.Poll(0, SelectMode.SelectError);
				} catch (ObjectDisposedException) {
					//TODO: why do we get this?
					return false;
				}
			}
		}

		public DateTime? LastSendStarted {
			get {
				var ticks = Interlocked.Read(ref _lastSendStarted);
				return ticks >= 0 ? new DateTime(ticks) : (DateTime?)null;
			}
		}

		public DateTime? LastReceiveStarted {
			get {
				var ticks = Interlocked.Read(ref _lastReceiveStarted);
				return ticks >= 0 ? new DateTime(ticks) : (DateTime?)null;
			}
		}

		private Socket _socket;
		private readonly IPEndPoint _remoteEndPoint;
		private readonly Action<ITcpConnection, TcpPackage> _handlePackage;
		private readonly Action<ITcpConnection, Exception> _onError;
		private readonly ILogger _log;
		private IPEndPoint _localEndPoint;

		private long _lastSendStarted = -1;
		private long _lastReceiveStarted = -1;
		private volatile bool _isClosed;

		private int _pendingSendBytes;
		private int _inSendBytes;
		private int _pendingReceivedBytes;
		private long _totalBytesSent;
		private long _totalBytesReceived;

		private int _sentAsyncs;
		private int _sentAsyncCallbacks;
		private int _recvAsyncs;
		private int _recvAsyncCallbacks;
		private readonly LengthPrefixMessageFramer _framer;

		public TcpConnectionBase(IPEndPoint remoteEndPoint,
			Action<ITcpConnection, TcpPackage> handlePackage,
			Action<ITcpConnection, Exception> onError, ILogger log) {
			Ensure.NotNull(remoteEndPoint, "remoteEndPoint");
			Ensure.NotNull(handlePackage, nameof(handlePackage));
			Ensure.NotNull(onError, nameof(onError));
			Ensure.NotNull(log, nameof(log));
			_remoteEndPoint = remoteEndPoint;
			_handlePackage = handlePackage;
			_onError = onError;
			_log = log;
			_framer = new LengthPrefixMessageFramer();
			_framer.RegisterMessageArrivedCallback(IncomingMessageArrived);

			TcpConnectionMonitor.Default.Register(this);
		}

		public void StartReceiving() => ReceiveAsync(OnRawDataReceived);

		private void IncomingMessageArrived(ArraySegment<byte> data) {
			var package = new TcpPackage();
			var valid = false;
			try {
				package = TcpPackage.FromArraySegment(data);
				valid = true;
				_handlePackage(this, package);
			} catch (Exception e) {
				this.Close(string.Format("Error when processing TcpPackage {0}: {1}",
					valid ? package.Command.ToString() : "<invalid package>", e.Message));

				var message = string.Format(
					"TcpPackageConnection: [{0}, L{1}, {2}] ERROR for {3}. Connection will be closed.",
					RemoteEndPoint, LocalEndPoint, ConnectionId,
					valid ? package.Command.ToString() : "<invalid package>");
				if (_onError != null)
					_onError(this, e);
				_log.Debug(e, message);
			}
		}


		private void OnRawDataReceived(ITcpConnection connection, IEnumerable<ArraySegment<byte>> data) {
			try {
				_framer.UnFrameData(data);
			} catch (PackageFramingException exc) {
				_log.Error(exc, "TcpPackageConnection: [{0}, L{1}, {2:B}]. Invalid TCP frame received.", RemoteEndPoint,
					LocalEndPoint, connection.ConnectionId);
				Close("Invalid TCP frame received.");
				return;
			}

			//NOTE: important to be the last statement in the callback
			connection.ReceiveAsync(OnRawDataReceived);
		}

		protected void InitConnectionBase(Socket socket) {
			Ensure.NotNull(socket, "socket");

			_socket = socket;
			_localEndPoint = Helper.EatException(() => (IPEndPoint)socket.LocalEndPoint);
		}

		protected void NotifySendScheduled(int bytes) {
			Interlocked.Add(ref _pendingSendBytes, bytes);
		}

		protected void NotifySendStarting(int bytes) {
			if (Interlocked.CompareExchange(ref _lastSendStarted, DateTime.UtcNow.Ticks, -1) != -1)
				throw new Exception("Concurrent send detected.");
			Interlocked.Add(ref _pendingSendBytes, -bytes);
			Interlocked.Add(ref _inSendBytes, bytes);
			Interlocked.Increment(ref _sentAsyncs);
		}

		protected void NotifySendCompleted(int bytes) {
			Interlocked.Exchange(ref _lastSendStarted, -1);
			Interlocked.Add(ref _inSendBytes, -bytes);
			Interlocked.Add(ref _totalBytesSent, bytes);
			Interlocked.Increment(ref _sentAsyncCallbacks);
		}

		protected void NotifyReceiveStarting() {
			if (Interlocked.CompareExchange(ref _lastReceiveStarted, DateTime.UtcNow.Ticks, -1) != -1)
				throw new Exception("Concurrent receive detected.");
			Interlocked.Increment(ref _recvAsyncs);
		}

		protected void NotifyReceiveCompleted(int bytes) {
			Interlocked.Exchange(ref _lastReceiveStarted, -1);
			Interlocked.Add(ref _pendingReceivedBytes, bytes);
			Interlocked.Add(ref _totalBytesReceived, bytes);
			Interlocked.Increment(ref _recvAsyncCallbacks);
		}

		protected void NotifyReceiveDispatched(int bytes) {
			Interlocked.Add(ref _pendingReceivedBytes, -bytes);
		}

		protected void NotifyClosed() {
			_isClosed = true;
			TcpConnectionMonitor.Default.Unregister(this);
		}
		
		public void Close(string reason) {
			CloseInternal(SocketError.Success, reason ?? "Normal socket close."); // normal socket closing
		}

		protected abstract void CloseInternal(SocketError success, string normalSocketClose);

		protected abstract void EnqueueSend(IEnumerable<ArraySegment<byte>> data); 

		public void EnqueueSend(TcpPackage package) => EnqueueSend(_framer.FrameData(package.AsArraySegment()));

		public abstract void ReceiveAsync(Action<ITcpConnection, IEnumerable<ArraySegment<byte>>> callback);
	}
}
