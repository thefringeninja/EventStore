using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace EventStore.Transport.Http {
	public interface ICodec {
		string ContentType { get; }
		Encoding Encoding { get; }
		bool CanParse(MediaType format);
		bool SuitableForResponse(MediaType component);
		bool HasEventIds { get; }
		bool HasEventTypes { get; }

		[Obsolete]
		T From<T>(string text);

		[Obsolete]
		string To<T>(T value);

		ValueTask<T> ReadAsync<T>(Stream stream, CancellationToken cancellationToken = default) =>
			throw new NotImplementedException();

		ValueTask WriteAsync(object response, Stream stream, CancellationToken cancellationToken = default) =>
			throw new NotImplementedException();
	}
}
