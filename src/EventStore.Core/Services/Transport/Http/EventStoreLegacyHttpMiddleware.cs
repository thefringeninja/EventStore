using System;
using System.Threading.Tasks;
using EventStore.Core.Bus;
using EventStore.Transport.Http;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using HttpResponse = Microsoft.AspNetCore.Http.HttpResponse;

namespace EventStore.Core.Services.Transport.Http {
	internal static class EventStoreLegacyHttpMiddleware {
		private const string EventStoreResponse = nameof(EventStoreResponse);

		private const string EventStoreRequestCodec = nameof(EventStoreRequestCodec);
		private const string EventStoreResponseCodec = nameof(EventStoreResponseCodec);

		public static IApplicationBuilder UseLegacyHttp(this IApplicationBuilder builder, IPublisher monitoringQueue) {
			if (builder == null) {
				throw new ArgumentNullException(nameof(builder));
			}

			if (monitoringQueue == null) {
				throw new ArgumentNullException(nameof(monitoringQueue));
			}

			return builder
					.UseRouting()
					.UseResponseCompression()
					.UseContentNegotiation()
					.UseHistograms("/histogram")
					.UseStats("/stats", monitoringQueue)
				;
		}

		public static IApplicationBuilder UseContentNegotiation(this IApplicationBuilder builder) {
			if (builder == null) {
				throw new ArgumentNullException(nameof(builder));
			}

			return builder.Use(async (context, next) => {
				var metadata = context.GetEndpoint()?.Metadata;

				if (metadata == null) {
					await next().ConfigureAwait(false);
					return;
				}

				if (RequestMethodHasPayload()) {
					var requestCodec = metadata.GetMetadata<RequestCodecs>().Select(context.Request);
					if (requestCodec == null) {
						context.Response.StatusCode = 415;
						return;
					}

					context.Items[EventStoreRequestCodec] = requestCodec;
				}

				var responseCodec = metadata.GetMetadata<ResponseCodecs>().Select(context.Request);

				if (responseCodec == null) {
					context.Response.StatusCode = 406;
					return;
				}

				await next().ConfigureAwait(false);

				if (context.Items.TryGetValue(EventStoreResponse, out var response) &&
				    response != null) {
					if (context.Items.TryGetValue(EventStoreResponseCodec, out var forcedCodec) &&
					    forcedCodec != null) {
						responseCodec = (ICodec)forcedCodec;
					}

					context.Response.ContentType = responseCodec.ContentType;

					await responseCodec.WriteAsync(response, context.Response.Body, context.RequestAborted)
						.ConfigureAwait(false);
				}

				bool RequestMethodHasPayload() => context.Request.Method?.ToUpperInvariant() switch {
					"PUT" => true,
					"POST" => true,
					"DELETE" => true, // what?
					"PATCH" => true,
					_ => false
				};
			});
		}

		public static void SetEventStoreResponse(this HttpContext context, object response, ICodec codec = null) {
			context.Items[EventStoreResponse] = response;
			context.Items[EventStoreResponseCodec] = codec;
		}

		public static ValueTask<T> GetEventStoreRequest<T>(this HttpContext context) {
			return !context.Items.TryGetValue(EventStoreRequestCodec, out var o) || !(o is ICodec requestCodec)
				? throw new Exception()
				: requestCodec.ReadAsync<T>(context.Request.Body, context.RequestAborted);
		}

		public static void Cache(this HttpResponse response, TimeSpan? duration, bool isPublic = true) =>
			response.Headers.Add(
				"cache-control",
				duration.HasValue
					? $"max-age={(int)duration.Value.TotalSeconds}, {(isPublic ? "public" : "private")}"
					: "max-age=0, no-cache, must-revalidate");
	}
}
