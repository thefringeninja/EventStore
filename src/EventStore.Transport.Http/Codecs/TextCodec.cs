using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using Microsoft.AspNetCore.Http;

namespace EventStore.Transport.Http.Codecs {
	public class TextCodec : ICodec {
		public string ContentType { get; } = Http.ContentType.PlainText;
		public Encoding Encoding { get; } = Helper.UTF8NoBom;
		public bool HasEventIds { get; } = false;
		public bool HasEventTypes { get; } = false;
		public bool CanParse(MediaType format) => format != null && format.Matches(ContentType, Encoding);

		public bool SuitableForResponse(MediaType component) =>
			component.Type == "*"
			|| string.Equals(component.Type, "text", StringComparison.OrdinalIgnoreCase)
			&& (component.Subtype == "*"
			    || string.Equals(component.Subtype, "plain", StringComparison.OrdinalIgnoreCase));

		public T From<T>(string text) => throw new NotSupportedException();

		public string To<T>(T value) => (object)value != null ? value.ToString() : null;

		public async ValueTask<T> ReadAsync<T>(Stream stream, CancellationToken cancellationToken = default) =>
			throw new NotSupportedException();

		public async ValueTask WriteAsync(object response, Stream stream,
			CancellationToken cancellationToken = default) {
			await stream.WriteAsync(Encoding.GetBytes(response?.ToString() ?? string.Empty), cancellationToken);
		}
	}
}
