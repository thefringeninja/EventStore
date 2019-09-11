using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace EventStore.Transport.Http.EntityManagement {
	public interface IHttpRequest {
		IAsyncResult BeginGetClientCertificate(AsyncCallback requestCallback, object state);
		X509Certificate2 EndGetClientCertificate(IAsyncResult asyncResult);
		X509Certificate2 GetClientCertificate();
		Task<X509Certificate2> GetClientCertificateAsync();
		string[] AcceptTypes { get; }
		int ClientCertificateError { get; }
		Encoding ContentEncoding { get; }
		long ContentLength64 { get; }
		string ContentType { get; }
		CookieCollection Cookies { get; }
		bool HasEntityBody { get; }
		NameValueCollection Headers { get; }
		string HttpMethod { get; }
		Stream InputStream { get; }
		bool IsAuthenticated { get; }
		bool IsLocal { get; }
		bool IsSecureConnection { get; }
		bool IsWebSocketRequest { get; }
		bool KeepAlive { get; }
		IPEndPoint LocalEndPoint { get; }
		Version ProtocolVersion { get; }
		NameValueCollection QueryString { get; }
		string RawUrl { get; }
		IPEndPoint RemoteEndPoint { get; }
		Guid RequestTraceIdentifier { get; }
		string ServiceName { get; }
		TransportContext TransportContext { get; }
		Uri Url { get; }
		Uri UrlReferrer { get; }
		string UserAgent { get; }
		string UserHostAddress { get; }
		string UserHostName { get; }
		string[] UserLanguages { get; }
	}
}
