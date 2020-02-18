using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using EventStore.Transport.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace EventStore.Core.Services.Transport.Http {
	public class ResponseCodecs : IReadOnlyList<ICodec> {
		private readonly IReadOnlyList<ICodec> _inner;

		public int Count => _inner.Count;
		public ICodec this[int index] => _inner[index];

		public ResponseCodecs(IReadOnlyList<ICodec> inner) {
			if (inner == null) {
				throw new ArgumentNullException(nameof(inner));
			}

			if (inner.Count == 0) {
				throw new ArgumentException(nameof(inner));
			}

			_inner = inner;
		}

		public ICodec Select(HttpRequest request) {
			if (!request.Headers.TryGetValue("accept", out var acceptHeaders) ||
			    StringValues.IsNullOrEmpty(acceptHeaders)) {
				return _inner[0];
			}

			return (from acceptHeader in acceptHeaders
				let mediaType = MediaType.TryParse(acceptHeader)
				where mediaType != null
				orderby mediaType.Priority descending
				from codec in _inner
				where codec.SuitableForResponse(mediaType)
				select codec).FirstOrDefault();
		}

		public IEnumerator<ICodec> GetEnumerator() => _inner.GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)_inner).GetEnumerator();

		public static implicit operator ResponseCodecs(ICodec[] codecs) =>
			new ResponseCodecs(codecs);
	}
}
