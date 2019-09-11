using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Transport.Http.EntityManagement {
	public class HttpListenerRequestAdapter : IHttpRequest {
		private readonly HttpListenerRequest _inner;

		public IAsyncResult BeginGetClientCertificate(AsyncCallback requestCallback, object state)
			=> _inner.BeginGetClientCertificate(requestCallback, state);

		public X509Certificate2 EndGetClientCertificate(IAsyncResult asyncResult)
			=> _inner.EndGetClientCertificate(asyncResult);

		public X509Certificate2 GetClientCertificate()
			=> _inner.GetClientCertificate();

		public Task<X509Certificate2> GetClientCertificateAsync()
			=> _inner.GetClientCertificateAsync();

		public string[] AcceptTypes => _inner.AcceptTypes;

		public int ClientCertificateError => _inner.ClientCertificateError;

		public Encoding ContentEncoding => _inner.ContentEncoding;

		public long ContentLength64 => _inner.ContentLength64;

		public string ContentType => _inner.ContentType;

		public CookieCollection Cookies => _inner.Cookies;

		public bool HasEntityBody => _inner.HasEntityBody;

		public NameValueCollection Headers => _inner.Headers;

		public string HttpMethod => _inner.HttpMethod;

		public Stream InputStream => _inner.InputStream;

		public bool IsAuthenticated => _inner.IsAuthenticated;

		public bool IsLocal => _inner.IsLocal;

		public bool IsSecureConnection => _inner.IsSecureConnection;

		public bool IsWebSocketRequest => _inner.IsWebSocketRequest;

		public bool KeepAlive => _inner.KeepAlive;

		public IPEndPoint LocalEndPoint => _inner.LocalEndPoint;

		public Version ProtocolVersion => _inner.ProtocolVersion;

		public NameValueCollection QueryString => _inner.QueryString;

		public string RawUrl => _inner.RawUrl;

		public IPEndPoint RemoteEndPoint => _inner.RemoteEndPoint;

		public Guid RequestTraceIdentifier => _inner.RequestTraceIdentifier;

		public string ServiceName => _inner.ServiceName;

		public TransportContext TransportContext => _inner.TransportContext;

		public Uri Url => _inner.Url;

		public Uri UrlReferrer => _inner.UrlReferrer;

		public string UserAgent => _inner.UserAgent;

		public string UserHostAddress => _inner.UserHostAddress;

		public string UserHostName => _inner.UserHostName;

		public string[] UserLanguages => _inner.UserLanguages;

		public HttpListenerRequestAdapter(HttpListenerRequest inner) {
			_inner = inner;
		}
	}
}
