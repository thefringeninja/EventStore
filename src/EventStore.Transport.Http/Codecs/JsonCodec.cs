using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using JsonConverter = Newtonsoft.Json.JsonConverter;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace EventStore.Transport.Http.Codecs {
	public class JsonCodec : ICodec {
		public static Formatting Formatting = Formatting.Indented;

		private static readonly ILogger Log = LogManager.GetLoggerFor<JsonCodec>();

		private static readonly JsonSerializerSettings FromSettings = new JsonSerializerSettings {
			ContractResolver = new CamelCasePropertyNamesContractResolver(),
			DateParseHandling = DateParseHandling.None,
			NullValueHandling = NullValueHandling.Ignore,
			DefaultValueHandling = DefaultValueHandling.Include,
			MissingMemberHandling = MissingMemberHandling.Ignore,
			TypeNameHandling = TypeNameHandling.None,
			MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
			Converters = new JsonConverter[] {
				new StringEnumConverter()
			}
		};

		public static readonly JsonSerializerSettings ToSettings = new JsonSerializerSettings {
			ContractResolver = new CamelCasePropertyNamesContractResolver(),
			DateFormatHandling = DateFormatHandling.IsoDateFormat,
			NullValueHandling = NullValueHandling.Ignore,
			DefaultValueHandling = DefaultValueHandling.Include,
			MissingMemberHandling = MissingMemberHandling.Ignore,
			TypeNameHandling = TypeNameHandling.None,
			Converters = new JsonConverter[] {new StringEnumConverter()}
		};

		private static readonly JsonSerializerOptions ToOptions = new JsonSerializerOptions {
			WriteIndented = true,
			PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
			IgnoreNullValues = true,
			Converters = {
				new JsonStringEnumConverter(),
				//new DictionaryConverter()
			}
		};

		public string ContentType { get; } = Http.ContentType.Json;
		public Encoding Encoding { get; } = Helper.UTF8NoBom;
		public bool HasEventIds { get; } = false;
		public bool HasEventTypes { get; } = false;

		public bool CanParse(MediaType format) => format != null && format.Matches(ContentType, Encoding);

		public bool SuitableForResponse(MediaType component) =>
			component.Type == "*"
			|| string.Equals(component.Type, "application", StringComparison.OrdinalIgnoreCase)
			&& (component.Subtype == "*"
			    || string.Equals(component.Subtype, "json", StringComparison.OrdinalIgnoreCase));

		public T From<T>(string text) {
			try {
				return JsonConvert.DeserializeObject<T>(text, FromSettings);
			} catch (Exception e) {
				Log.ErrorException(e, "'{text}' is not a valid serialized {type}", text, typeof(T).FullName);
				return default(T);
			}
		}

		public string To<T>(T value) {
			if (value == null)
				return "";

			if ((object)value == Empty.Result)
				return Empty.Json;

			try {
				return JsonConvert.SerializeObject(value, Formatting, ToSettings);
			} catch (Exception ex) {
				Log.ErrorException(ex, "Error serializing object {value}", value);
				return null;
			}
		}

		public ValueTask<T> ReadAsync<T>(Stream stream, CancellationToken cancellationToken = default) =>
			JsonSerializer.DeserializeAsync<T>(stream, ToOptions, cancellationToken);

		public ValueTask WriteAsync(object response, Stream stream, CancellationToken cancellationToken = default) =>
			response == Empty.Result
				? stream.WriteAsync(Encoding.GetBytes(Empty.Json), cancellationToken)
				: new ValueTask(JsonSerializer.SerializeAsync(stream, response, response?.GetType() ?? typeof(object),
					ToOptions, cancellationToken));
	}
}
