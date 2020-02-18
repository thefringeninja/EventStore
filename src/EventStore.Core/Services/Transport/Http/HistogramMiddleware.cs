using System;
using System.IO;
using System.Threading.Tasks;
using EventStore.Core.Services.Histograms;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using HdrHistogram;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace EventStore.Core.Services.Transport.Http {
	internal static class HistogramMiddleware {
		public static readonly ResponseCodecs Acceptable = new ICodec[]
			{Codec.Json, Codec.Xml, Codec.ApplicationXml, Codec.Text};

		public static IApplicationBuilder UseHistograms(this IApplicationBuilder builder, PathString path) {
			if (builder == null) throw new ArgumentNullException(nameof(builder));
			return builder.UseEndpoints(endpoints => endpoints
				.MapGet($"{path}/{{name}}", OnGetHistogram)
				.WithMetadata(AuthorizationLevel.Ops, RequestCodecs.None, new ResponseCodecs(Acceptable)));
		}

		private static async Task OnGetHistogram(HttpContext context) {
			var data = context.GetRouteData();
			var name = data.Values["name"] as string;

			var histogram = HistogramService.GetHistogram(name);

			if (histogram == null) {
				context.Response.StatusCode = 404;

				return;
			}

			await using var stream = new MemoryStream();
			await using var writer = new StreamWriter(stream);

			lock (histogram) {
				context.Response.StatusCode = 200;
				context.Response.ContentType = Codec.Text.ContentType;
				histogram.OutputPercentileDistribution(writer, outputValueUnitScalingRatio: 1_000_000);
			}

			stream.Position = 0;
			await stream.CopyToAsync(context.Response.Body, context.RequestAborted).ConfigureAwait(false);
		}
	}
}
