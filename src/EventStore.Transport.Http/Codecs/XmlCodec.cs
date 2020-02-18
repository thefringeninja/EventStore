using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using EventStore.Common.Log;
using EventStore.Common.Utils;

namespace EventStore.Transport.Http.Codecs {
	public class XmlCodec : ICodec {
		private static readonly ILogger Log = LogManager.GetLoggerFor<XmlCodec>();

		public string ContentType { get; } = EventStore.Transport.Http.ContentType.Xml;
		public Encoding Encoding { get; } = Helper.UTF8NoBom;
		public bool HasEventIds { get; } = false;
		public bool HasEventTypes { get; } = false;
		public bool CanParse(MediaType format) => format != null && format.Matches(ContentType, Encoding);

		public bool SuitableForResponse(MediaType component) =>
			component.Type == "*"
			|| string.Equals(component.Type, "text", StringComparison.OrdinalIgnoreCase)
			&& (component.Subtype == "*"
			    || string.Equals(component.Subtype, "xml", StringComparison.OrdinalIgnoreCase));

		public T From<T>(string text) {
			if (string.IsNullOrEmpty(text))
				return default;

			try {
				using var reader = new StringReader(text);
				return (T)new XmlSerializer(typeof(T)).Deserialize(reader);
			} catch (Exception e) {
				Log.ErrorException(e, "'{text}' is not a valid serialized {type}", text, typeof(T).FullName);
				return default;
			}
		}

		public string To<T>(T value) {
			if (value == null)
				return null;

			if ((object)value == Empty.Result)
				return Empty.Xml;

			var type = typeof(T);

			try {
				using var memory = new MemoryStream();
				using var writer = new XmlTextWriter(memory, Encoding);
				if (value is IXmlSerializable serializable) {
					writer.WriteStartDocument();
					serializable.WriteXml(writer);
					writer.WriteEndDocument();
				} else {
					new XmlSerializer(type).Serialize(writer, value);
				}

				writer.Flush();
				return Encoding.GetString(memory.GetBuffer(), 0, (int)memory.Length);
			} catch (Exception exc) {
				Log.ErrorException(exc, "Error serializing object of type {type}", value.GetType().FullName);
				return null;
			}
		}

		public async ValueTask<T> ReadAsync<T>(Stream stream, CancellationToken cancellationToken = default) =>
			From<T>(await new StreamReader(stream, Encoding).ReadToEndAsync());

		public ValueTask WriteAsync(object response, Stream stream, CancellationToken cancellationToken = default) {
			if (response == null || response == Empty.Result) {
				return new ValueTask(Task.CompletedTask);
			}

			try {
				using var memory = new MemoryStream();
				using var writer = new XmlTextWriter(memory, Encoding);
				if (response is IXmlSerializable serializable) {
					writer.WriteStartDocument();
					serializable.WriteXml(writer);
					writer.WriteEndDocument();
				} else {
					new XmlSerializer(response.GetType()).Serialize(writer, response);
				}

				writer.Flush();

				return new ValueTask(memory.CopyToAsync(stream, cancellationToken));
			} catch (Exception exc) {
				Log.ErrorException(exc, "Error serializing object of type {type}", response.GetType().FullName);
				return new ValueTask(Task.CompletedTask);
			}
		}
	}
}
