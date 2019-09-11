using System;
using System.IO;
using System.Net;
using System.Text;

namespace EventStore.Transport.Http.EntityManagement {
	public class HttpListenerResponseAdapter : IHttpResponse {
		private readonly HttpListenerResponse _inner;

		public void Abort() => _inner.Abort();

		public void AddHeader(string name, string value) => _inner.AddHeader(name, value);

		public void AppendCookie(Cookie cookie) => _inner.AppendCookie(cookie);

		public void AppendHeader(string name, string value) => _inner.AppendHeader(name, value);

		public void Close() => _inner.Close();

		public void Close(byte[] responseEntity, bool willBlock) => _inner.Close(responseEntity, willBlock);

		public void CopyFrom(HttpListenerResponse templateResponse) => _inner.CopyFrom(templateResponse);

		public void Redirect(string url) => _inner.Redirect(url);

		public void SetCookie(Cookie cookie) => _inner.SetCookie(cookie);

		public Encoding ContentEncoding {
			get => _inner.ContentEncoding;
			set => _inner.ContentEncoding = value;
		}

		public long ContentLength64 {
			get => _inner.ContentLength64;
			set => _inner.ContentLength64 = value;
		}

		public string ContentType {
			get => _inner.ContentType;
			set => _inner.ContentType = value;
		}

		public CookieCollection Cookies {
			get => _inner.Cookies;
			set => _inner.Cookies = value;
		}

		public WebHeaderCollection Headers {
			get => _inner.Headers;
			set => _inner.Headers = value;
		}

		public bool KeepAlive {
			get => _inner.KeepAlive;
			set => _inner.KeepAlive = value;
		}

		public Stream OutputStream => _inner.OutputStream;

		public Version ProtocolVersion {
			get => _inner.ProtocolVersion;
			set => _inner.ProtocolVersion = value;
		}

		public string RedirectLocation {
			get => _inner.RedirectLocation;
			set => _inner.RedirectLocation = value;
		}

		public bool SendChunked {
			get => _inner.SendChunked;
			set => _inner.SendChunked = value;
		}

		public int StatusCode {
			get => _inner.StatusCode;
			set => _inner.StatusCode = value;
		}

		public string StatusDescription {
			get => _inner.StatusDescription;
			set => _inner.StatusDescription = value;
		}

		public HttpListenerResponseAdapter(HttpListenerResponse inner) {
			_inner = inner;
		}
	}
}
