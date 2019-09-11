using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace EventStore.Transport.Http.EntityManagement {
	public class CoreHttpRequestAdapter : IHttpRequest {
		private readonly HttpRequest _inner;

		public CoreHttpRequestAdapter(HttpRequest inner) {
			_inner = inner;
		}

		public string[] AcceptTypes => throw new NotImplementedException();

		public long ContentLength64 => _inner.ContentLength ?? 0;

		public string ContentType => _inner.ContentType;

		public string HttpMethod => _inner.Method;

		public Stream InputStream => _inner.Body;

		public string RawUrl => throw new NotImplementedException();

		public IPEndPoint RemoteEndPoint => new IPEndPoint(
			_inner.HttpContext.Connection.RemoteIpAddress, _inner.HttpContext.Connection.RemotePort);

		public Uri Url => throw new NotImplementedException();

		public IEnumerable<string> GetQueryStringKeys() => _inner.Query.Keys;

		public StringValues GetQueryStringValues(string key) => _inner.Query[key];

		public IEnumerable<string> GetHeaderKeys() => _inner.Headers.Keys;
		public StringValues GetHeaderValues(string key) => _inner.Headers[key];
	}
}
