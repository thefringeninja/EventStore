using System;
using System.Net.Http;
using System.Threading.Tasks;
using EventStore.ClusterNode;
using EventStore.Core;
using EventStore.Core.TransactionLog.Chunks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventStore.Transport.Http {
	public abstract class EventStoreHttpTransportFixture : IAsyncLifetime {
		private readonly TFChunkDb _db;

		protected TestServer TestServer { get; }
		public ClusterVNode Node { get; }
		public HttpClient Client { get; }

		protected EventStoreHttpTransportFixture(
			Action<VNodeBuilder> configureVNode = default,
			Action<IWebHostBuilder> configureWebHost = default) {
			var webHostBuilder = new WebHostBuilder();
			configureWebHost?.Invoke(webHostBuilder);

			var vNodeBuilder = new TestVNodeBuilder();
			vNodeBuilder.RunInMemory().WithTfChunkSize(1024 * 1024);
			configureVNode?.Invoke(vNodeBuilder);

			Node = vNodeBuilder.Build();
			_db = vNodeBuilder.GetDb();

			TestServer = new TestServer(
				webHostBuilder
					.UseStartup(new TestClusterVNodeStartup(Node)));

			Client = TestServer.CreateClient();
		}

		protected abstract Task Given();
		protected abstract Task When();

		public virtual async Task InitializeAsync() {
			await Node.StartAsync(true);
			await Given().WithTimeout(TimeSpan.FromMinutes(5));
			await When().WithTimeout(TimeSpan.FromMinutes(5));
		}

		public virtual async Task DisposeAsync() {
			await Node.StopAsync();
			_db.Dispose();
			TestServer.Dispose();
			Client.Dispose();
		}

		private class TestClusterVNodeStartup : IStartup {
			private readonly ClusterVNode _node;

			public TestClusterVNodeStartup(ClusterVNode node) {
				if (node == null) throw new ArgumentNullException(nameof(node));
				_node = node;
			}

			public IServiceProvider ConfigureServices(IServiceCollection services) =>
				_node.Startup.ConfigureServices(services);

			public void Configure(IApplicationBuilder app) => _node.Startup.Configure(app.Use(CompleteResponse));

			private static RequestDelegate CompleteResponse(RequestDelegate next) => context =>
				next(context).ContinueWith(_ => context.Response.Body.FlushAsync());
		}

		private class TestVNodeBuilder : ClusterVNodeBuilder {
			public TFChunkDb GetDb() => _db;
		}
	}
}
