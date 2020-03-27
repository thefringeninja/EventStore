using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace EventStore.ClusterNode {
	internal class Program {
		private readonly string[] _args;
		private readonly CancellationTokenSource _cts;
		private readonly TaskCompletionSource<int> _exitCodeSource;
		private readonly ClusterVNodeHostedService _hostedService;

		public static Task<int> Main(string[] args) {
			return new Program(args).Run();
		}

		private Program(string[] args) {
			_args = args ?? Array.Empty<string>();
			_cts = new CancellationTokenSource();
			_exitCodeSource = new TaskCompletionSource<int>();
			_hostedService = new ClusterVNodeHostedService(_args);

			Console.CancelKeyPress += delegate {
				Application.Exit(0, "Cancelled.");
			};
		}

		public async Task<int> Run() {
			try {
				using var host = new HostBuilder()
					.ConfigureHostConfiguration(builder =>
						builder.AddEnvironmentVariables("DOTNET_").AddCommandLine(_args))
					.ConfigureAppConfiguration(builder =>
						builder.AddEnvironmentVariables().AddCommandLine(_args))
					.ConfigureServices(services => services
						.AddSingleton<IHostedService>(_hostedService))
					.ConfigureLogging(logging => logging.AddSerilog())
					.ConfigureWebHostDefaults(builder =>
						builder.UseKestrel(server => {
								server.Listen(_hostedService.Options.IntIp, _hostedService.Options.IntHttpPort,
									listenOptions => listenOptions.UseHttps(_hostedService.Node.Certificate));
								server.Listen(_hostedService.Options.ExtIp, _hostedService.Options.ExtHttpPort,
									listenOptions => listenOptions.UseHttps(_hostedService.Node.Certificate));
							})
							.ConfigureServices(services => _hostedService.Node.Startup.ConfigureServices(services))
							.Configure(_hostedService.Node.Startup.Configure))
					.Build();

				var applicationLifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
				await using var _ = applicationLifetime.ApplicationStarted.Register(() => {
					Application.RegisterExitAction(code => {
						_exitCodeSource.SetResult(code);
						applicationLifetime.StopApplication();
					});
				});

				await host.RunAsync(_cts.Token);

				return await _exitCodeSource.Task;
			} catch (Exception ex) {
				Log.Fatal(ex, "Host terminated unexpectedly.");
				return 1;
			} finally {
				Log.CloseAndFlush();
			}
		}
	}
}
