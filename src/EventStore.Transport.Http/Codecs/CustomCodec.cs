using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Utils;

namespace EventStore.Transport.Http.Codecs {
	public class CustomCodec : ICodec {
		public ICodec BaseCodec { get; }
		public string ContentType { get; }
		public Encoding Encoding { get; }
		public bool HasEventIds { get; }
		public bool HasEventTypes { get; }

		private readonly string _type;
		private readonly string _subtype;

		internal CustomCodec(ICodec codec, string contentType, Encoding encoding, bool hasEventIds,
			bool hasEventTypes) {
			Ensure.NotNull(codec, "codec");
			Ensure.NotNull(contentType, "contentType");
			HasEventTypes = hasEventTypes;
			HasEventIds = hasEventIds;
			BaseCodec = codec;
			ContentType = contentType;
			Encoding = encoding;
			var parts = contentType.Split(new[] {'/'}, 2);
			if (parts.Length != 2)
				throw new ArgumentException("contentType");
			_type = parts[0];
			_subtype = parts[1];
		}

		public bool CanParse(MediaType format) => format != null && format.Matches(ContentType, Encoding);

		public bool SuitableForResponse(MediaType component) =>
			component.Type == "*"
			|| string.Equals(component.Type, _type, StringComparison.OrdinalIgnoreCase)
			&& (component.Subtype == "*"
			    || string.Equals(component.Subtype, _subtype, StringComparison.OrdinalIgnoreCase));

		public T From<T>(string text) => BaseCodec.From<T>(text);

		public string To<T>(T value) => BaseCodec.To(value);

		public ValueTask<T> ReadAsync<T>(Stream stream, CancellationToken cancellationToken = default)
			=> BaseCodec.ReadAsync<T>(stream, cancellationToken);

		public ValueTask WriteAsync(object response, Stream stream, CancellationToken cancellationToken = default)
			=> BaseCodec.WriteAsync(response, stream, cancellationToken);
	}
}
