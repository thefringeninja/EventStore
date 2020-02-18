using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.Monitoring;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace EventStore.Core.Services.Transport.Http {
	internal static class StatsMiddleware {
		public static readonly ResponseCodecs Acceptable = new ICodec[]
			{Codec.Json};

		public static IApplicationBuilder UseStats(this IApplicationBuilder builder, PathString path,
			IPublisher monitoringQueue) {
			if (builder == null) throw new ArgumentNullException(nameof(builder));
			if (monitoringQueue == null) throw new ArgumentNullException(nameof(monitoringQueue));

			return builder.UseEndpoints(endpoints => {
				endpoints.MapGet(path, OnGetFreshStats)
					.WithMetadata(AuthorizationLevel.None, RequestCodecs.None, Acceptable);
				endpoints.MapGet($"{path}/replication", OnGetReplicationStats)
					.WithMetadata(AuthorizationLevel.None, RequestCodecs.None, Acceptable);
				endpoints.MapGet($"{path}/tcp", OnGetTcpConnectionStats)
					.WithMetadata(AuthorizationLevel.None, RequestCodecs.None, Acceptable);
				endpoints.MapGet($"{path}/{{**statPath}}", OnGetFreshStats)
					.WithMetadata(AuthorizationLevel.None, RequestCodecs.None, Acceptable);
			});

			Task OnGetTcpConnectionStats(HttpContext context) {
				var tcs = new TaskCompletionSource<bool>();
				var envelope = new CallbackEnvelope(OnCompleted);

				monitoringQueue.Publish(new MonitoringMessage.GetFreshTcpConnectionStats(envelope));

				return tcs.Task;

				void OnCompleted(Message message) {
					if (!(message is MonitoringMessage.GetFreshTcpConnectionStatsCompleted completed)) {
						tcs.TrySetResult(true);
						context.Response.StatusCode = 500;
						return;
					}

					context.Response.StatusCode = completed.ConnectionStats == null
						? 404
						: 200;

					context.Response.Cache(MonitoringService.MemoizePeriod);
					context.SetEventStoreResponse(completed.ConnectionStats);

					tcs.TrySetResult(true);
				}
			}

			Task OnGetReplicationStats(HttpContext context) {
				var tcs = new TaskCompletionSource<bool>();
				var envelope = new CallbackEnvelope(OnCompleted);

				monitoringQueue.Publish(new ReplicationMessage.GetReplicationStats(envelope));

				return tcs.Task;

				void OnCompleted(Message message) {
					if (!(message is ReplicationMessage.GetReplicationStatsCompleted completed)) {
						context.Response.StatusCode = 500;
						tcs.TrySetResult(true);
						return;
					}

					context.Response.Cache(MonitoringService.MemoizePeriod);
					context.SetEventStoreResponse(completed.ReplicationStats);

					tcs.TrySetResult(true);
				}
			}

			Task OnGetFreshStats(HttpContext context) {
				var tcs = new TaskCompletionSource<bool>();
				var envelope = new CallbackEnvelope(OnCompleted);

				var data = context.GetRouteData();

				var statPath = data.Values["statPath"] as string;
				bool.TryParse(context.Request.Query["metadata"], out var useMetadata);
				bool.TryParse(context.Request.Query["group"], out var useGrouping);

				if (!useGrouping && !string.IsNullOrEmpty(statPath)) {
					context.Response.StatusCode = 400;
					context.SetEventStoreResponse("Dynamic stats selection works only with grouping enabled",
						Codec.Text);

					return Task.CompletedTask;
				}

				monitoringQueue.Publish(
					new MonitoringMessage.GetFreshStats(envelope, GetStatSelector(), useMetadata, useGrouping));

				return tcs.Task;

				void OnCompleted(Message message) {
					if (!(message is MonitoringMessage.GetFreshStatsCompleted completed)) {
						context.Response.StatusCode = 500;
						tcs.TrySetResult(true);
						return;
					}

					if (completed.Success) {
						context.SetEventStoreResponse(completed.Stats);
						context.Response.Cache(MonitoringService.MemoizePeriod);
						context.Response.StatusCode = 200;
					} else {
						context.Response.StatusCode = 404;
					}

					tcs.TrySetResult(true);
				}

				Func<Dictionary<string, object>, Dictionary<string, object>> GetStatSelector() {
					if (string.IsNullOrEmpty(statPath))
						return dict => dict;

					//NOTE: this is fix for Mono incompatibility in UriTemplate behavior for /a/b{*C}
					//todo: use IsMono here?
					if (statPath.StartsWith("stats/")) {
						statPath = statPath.Substring(6);
						if (string.IsNullOrEmpty(statPath))
							return dict => dict;
					}

					var groups = statPath.Split('/');

					return dict => {
						if (dict == null) throw new ArgumentNullException(nameof(dict));


						foreach (string groupName in groups) {
							if (!dict.TryGetValue(groupName, out var item))
								return null;

							dict = item as Dictionary<string, object>;

							if (dict == null)
								return null;
						}

						return dict;
					};
				}
			}
		}
	}
}
