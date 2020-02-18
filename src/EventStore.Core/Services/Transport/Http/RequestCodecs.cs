using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using EventStore.Transport.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace EventStore.Core.Services.Transport.Http {
	public class RequestCodecs : IReadOnlyList<ICodec> {
		private readonly IReadOnlyList<ICodec> _inner;

		public static readonly RequestCodecs None = new RequestCodecs(Array.Empty<ICodec>());

		public int Count => _inner.Count;
		public ICodec this[int index] => _inner[index];

		public RequestCodecs(IReadOnlyList<ICodec> inner) {
			if (inner == null) {
				throw new ArgumentNullException(nameof(inner));
			}

			_inner = inner;
		}

		public ICodec Select(HttpRequest request) =>
			_inner.Count == 0 ||
			!request.Headers.TryGetValue("content-type", out var contentTypeHeader) ||
			StringValues.IsNullOrEmpty(contentTypeHeader) ||
			!MediaType.TryParse(contentTypeHeader, out var contentType)
				? null
				: _inner.FirstOrDefault(codec => codec.CanParse(contentType));

		public IEnumerator<ICodec> GetEnumerator() => _inner.GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)_inner).GetEnumerator();

		public static implicit operator RequestCodecs(ICodec[] codecs) =>
			new RequestCodecs(codecs);
	}
}
