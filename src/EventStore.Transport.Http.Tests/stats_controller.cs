using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using EventStore.Core.Messages;
using EventStore.Core.Services.Monitoring;
using EventStore.Core.Services.Transport.Http;
using EventStore.Transport.Http.Codecs;
using Xunit;
using static System.Net.HttpStatusCode;
using static System.Net.Http.HttpMethod;

namespace EventStore.Transport.Http {
	public class stats_controller : IClassFixture<stats_controller.Fixture> {
		private static readonly string[] Paths = {
			"/stats",
			"/stats/tcp",
			"/stats/replication",
			"/stats/extra/stats?"
		};

		private readonly Fixture _fixture;

		public stats_controller(Fixture fixture) {
			_fixture = fixture;
		}

		public static IEnumerable<object[]> NotAcceptableCases() =>
			from codec in Codec.All.Except(StatsMiddleware.Acceptable, CodecEqualityComparer.Instance)
			from path in Paths
			select new object[] {new MediaTypeWithQualityHeaderValue(codec.ContentType), path};

		[Theory, MemberData(nameof(NotAcceptableCases))]
		public async Task not_acceptable(MediaTypeWithQualityHeaderValue accept, string path) {
			using var response = await _fixture.Client.SendAsync(new HttpRequestMessage(Get, path) {
				Headers = {Accept = {accept}}
			});

			Assert.Equal(NotAcceptable, response.StatusCode);
		}

		[Fact]
		public async Task bad_request_when_use_grouping_without_named_stats() {
			using var response = await _fixture.Client.GetAsync("/stats/extra/stats?group=false");

			Assert.Equal(BadRequest, response.StatusCode);
			Assert.Equal("text/plain", response.Content.Headers.ContentType?.MediaType);
			Assert.Equal("Dynamic stats selection works only with grouping enabled",
				await response.Content.ReadAsStringAsync());
		}

		public static IEnumerable<object[]> AcceptableCases() =>
			from codec in StatsMiddleware.Acceptable
			select new object[] {new MediaTypeWithQualityHeaderValue(codec.ContentType), codec};

		[Theory, MemberData(nameof(AcceptableCases))]
		public async Task stats(MediaTypeWithQualityHeaderValue accept, ICodec responseCodec) {
			using var response = await _fixture.Client.SendAsync(new HttpRequestMessage(Get, "/stats") {
				Headers = {Accept = {accept}}
			});

			Assert.Equal(OK, response.StatusCode);
			Assert.Equal(new CacheControlHeaderValue {
				MaxAge = MonitoringService.MemoizePeriod,
				Public = true
			}, response.Headers.CacheControl);

			var stats = await responseCodec.ReadAsync<IDictionary<string, object>>(
				await response.Content.ReadAsStreamAsync());
			Assert.NotNull(stats);
			var procId = Assert.Contains("proc-id", stats);
			Assert.Equal(Process.GetCurrentProcess().Id.ToString(), procId?.ToString());
		}

		[Theory, MemberData(nameof(AcceptableCases))]
		public async Task tcp(MediaTypeWithQualityHeaderValue accept, ICodec responseCodec) {
			using var response = await _fixture.Client.SendAsync(new HttpRequestMessage(Get, "/stats/tcp") {
				Headers = {Accept = {accept}}
			});

			Assert.Equal(OK, response.StatusCode);
			Assert.Equal(new CacheControlHeaderValue {
				MaxAge = MonitoringService.MemoizePeriod,
				Public = true
			}, response.Headers.CacheControl);

			var stats = await responseCodec.ReadAsync<MonitoringMessage.TcpConnectionStats[]>(
				await response.Content.ReadAsStreamAsync());
			Assert.NotNull(stats);
		}

		[Theory(Skip = "Only works on a cluster"), MemberData(nameof(AcceptableCases))]
		public async Task replication(MediaTypeWithQualityHeaderValue accept, ICodec responseCodec) {
			using var response = await _fixture.Client.SendAsync(new HttpRequestMessage(Get, "/stats/replication") {
				Headers = {Accept = {accept}}
			});

			Assert.Equal(OK, response.StatusCode);
			Assert.Equal(new CacheControlHeaderValue {
				MaxAge = MonitoringService.MemoizePeriod,
				Public = true
			}, response.Headers.CacheControl);

			var stats = await responseCodec.ReadAsync<ReplicationMessage.ReplicationStats[]>(
				await response.Content.ReadAsStreamAsync());
			Assert.NotNull(stats);
		}

		[Theory, MemberData(nameof(AcceptableCases))]
		public async Task grouped_stats(MediaTypeWithQualityHeaderValue accept, ICodec responseCodec) {
			using var response = await _fixture.Client.SendAsync(
				new HttpRequestMessage(Get, "/stats/proc?group=true") {
					Headers = {Accept = {accept}}
				});
			Assert.Equal(OK, response.StatusCode);
			Assert.Equal(new CacheControlHeaderValue {
				MaxAge = MonitoringService.MemoizePeriod,
				Public = true
			}, response.Headers.CacheControl);

			var stats = await responseCodec.ReadAsync<IDictionary<string, object>>(
				await response.Content.ReadAsStreamAsync());
			Assert.NotNull(stats);
			var procId = Assert.Contains("id", stats);
			Assert.Equal(Process.GetCurrentProcess().Id.ToString(), procId?.ToString());
		}

		public class Fixture : EventStoreHttpTransportFixture {
			protected override Task Given() => Task.CompletedTask;
			protected override Task When() => Task.CompletedTask;
		}
	}

	internal class CodecEqualityComparer : IEqualityComparer<ICodec> {
		public static readonly IEqualityComparer<ICodec> Instance = new CodecEqualityComparer();
		public bool Equals(ICodec x, ICodec y) => ReferenceEquals(x, y);

		public int GetHashCode(ICodec obj) => obj.GetHashCode();
	}
}
