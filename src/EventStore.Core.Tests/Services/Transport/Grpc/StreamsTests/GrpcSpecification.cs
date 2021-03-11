using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.Client.Shared;
using EventStore.Client.Streams;
using EventStore.Core.Services.Transport.Grpc;
using EventStore.Core.Tests.Helpers;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;
using EventData = EventStore.ClientAPI.EventData;
using Convert = System.Convert;
using Streams = EventStore.Client.Streams.Streams;

namespace EventStore.Core.Tests.Services.Transport.Grpc.StreamsTests {
	public abstract class GrpcSpecification : IDisposable {
		private readonly TestServer _server;
		private readonly GrpcChannel _channel;
		private readonly IHost _host;
		private readonly MiniNode _node;
		protected MiniNode Node => _node;
		protected Streams.StreamsClient StreamsClient { get; }

		protected GrpcSpecification() {
			_node = new MiniNode(GetType().FullName, inMemDb: true);
			var builder = new HostBuilder()
				.ConfigureWebHostDefaults(webHost => webHost.UseTestServer()
					.ConfigureServices(services => _node.Node.Startup.ConfigureServices(services))
					.Configure(_node.Node.Startup.Configure));
			_host = builder.Start();
			_server = _host.GetTestServer();
			_channel = GrpcChannel.ForAddress(new UriBuilder {
				Scheme = Uri.UriSchemeHttps
			}.Uri, new GrpcChannelOptions {
				HttpClient = _server.CreateClient(),
				DisposeHttpClient = true
			});
			StreamsClient = new Streams.StreamsClient(_channel);
		}

		protected abstract Task Given();

		protected abstract Task When();

		[OneTimeSetUp]
		public async Task SetUp() {
			await _node.Start();
			await _node.AdminUserCreated;

			try {
				await Given().WithTimeout(TimeSpan.FromSeconds(10));
			} catch (Exception ex) {
				throw new Exception("Given Failed", ex);
			}

			await When().WithTimeout(TimeSpan.FromSeconds(10));
			try {
			} catch (Exception) {
			}
		}

		private static CallCredentials CallCredentialsFromUser((string userName, string password) credentials) =>
			CallCredentials.FromInterceptor((context, metadata) => {
				metadata.Add(new Metadata.Entry("authorization",
					$"Basic {Convert.ToBase64String(Encoding.ASCII.GetBytes($"{credentials.userName}:{credentials.password}"))}"));

				return Task.CompletedTask;
			});

		protected static (string userName, string password) AdminCredentials => ("admin", "changeit");

		protected virtual (string userName, string password) DefaultCredentials => default;

		protected CallOptions GetCallOptions((string userName, string password) credentials = default) {
			credentials = credentials == default ? DefaultCredentials : credentials;

			return new CallOptions(credentials: credentials == default ? null : CallCredentialsFromUser(credentials),
				deadline: Debugger.IsAttached
					? DateTime.UtcNow.AddDays(1)
					: new DateTime?());
		}

		protected async Task<AppendResp> AppendToStream(string streamName, IEnumerable<EventData> events,
			(string userName, string password) userCredentials = default) {
			using var call = StreamsClient.Append(GetCallOptions(userCredentials));

			await call.RequestStream.WriteAsync(new AppendReq {
				Options = new AppendReq.Types.Options {
					Any = new Empty(),
					StreamIdentifier = new StreamIdentifier {
						StreamName = ByteString.CopyFromUtf8(streamName)
					}
				}
			});

			foreach (var @event in events) {
				await call.RequestStream.WriteAsync(new AppendReq {
					ProposedMessage = new AppendReq.Types.ProposedMessage {
						Id = new UUID {
							String = Uuid.FromGuid(@event.EventId).ToString()
						},
						Metadata = {
							{Core.Services.Transport.Grpc.Constants.Metadata.Type, @event.Type}, {
								Core.Services.Transport.Grpc.Constants.Metadata.ContentType,
								@event.IsJson
									? Core.Services.Transport.Grpc.Constants.Metadata.ContentTypes.ApplicationJson
									: Core.Services.Transport.Grpc.Constants.Metadata.ContentTypes
										.ApplicationOctetStream
							}
						},
						CustomMetadata = ByteString.Empty,
						Data = ByteString.CopyFrom(@event.Data)
					}
				});
			}

			await call.RequestStream.CompleteAsync();

			return await call.ResponseAsync;
		}

		protected static IEnumerable<EventData> CreateEvents(int count) =>
			Enumerable.Range(0, count)
				.Select(_ => new EventData(Guid.NewGuid(), "type", false, Array.Empty<byte>(), null));

		public void Dispose() {
			_server?.Dispose();
			_channel?.Dispose();
			_host?.Dispose();
		}

		public abstract class Read : GrpcSpecification {
			private readonly List<ReadResp> _readResponses = new List<ReadResp>();
			protected virtual string StreamName { get; } = Guid.NewGuid().ToString();
			protected virtual int EventCount => 1;
			protected abstract ReadReq ReadRequest { get; }
			protected Position PositionOfLastWrite { get; private set; } = Position.Start;

			protected IReadOnlyList<ReadResp> ReadResponses {
				get => _readResponses;
			}

			protected virtual IAsyncEnumerator<ReadResp> GetEnumerator(IAsyncEnumerable<ReadResp> enumerable) =>
				enumerable.GetAsyncEnumerator();

			protected override async Task Given() {
				var appendResponse = await AppendToStream(StreamName, CreateEvents(EventCount));
				PositionOfLastWrite = new Position(appendResponse.Success.Position.CommitPosition,
					appendResponse.Success.Position.PreparePosition);
			}

			protected override async Task When() {
				using var call = StreamsClient.Read(ReadRequest, GetCallOptions());
				await using var enumerator = GetEnumerator(call.ResponseStream.ReadAllAsync());

				while (await enumerator.MoveNextAsync()) {
					_readResponses.Add(enumerator.Current);
				}
			}
		}

		protected class SubscriptionEnumerator : IAsyncEnumerator<ReadResp> {
			private readonly IAsyncEnumerator<ReadResp> _inner;
			private readonly Position _lastPosition;
			private bool _lastPositionReached;

			public SubscriptionEnumerator(IAsyncEnumerable<ReadResp> enumerable, Position lastPosition) {
				_inner = enumerable.GetAsyncEnumerator();
				_lastPosition = lastPosition;
				_lastPositionReached = false;
			}

			public ValueTask DisposeAsync() => _inner.DisposeAsync();

			public async ValueTask<bool> MoveNextAsync() {
				Loop:
				if (_lastPositionReached) {
					return false;
				}
				if (!await _inner.MoveNextAsync()) {
					return false;
				}

				if (Current.ContentCase != ReadResp.ContentOneofCase.Checkpoint &&
				    Current.ContentCase != ReadResp.ContentOneofCase.Event) {
					return true;
				}

				if (Current.ContentCase == ReadResp.ContentOneofCase.Checkpoint) {
					var position = new Position(Current.Checkpoint.CommitPosition,
						Current.Checkpoint.PreparePosition);
					return position <= _lastPosition;
				}


				if (Current.ContentCase == ReadResp.ContentOneofCase.Event) {
					var position = new Position(Current.Event.OriginalEvent.CommitPosition,
						Current.Event.OriginalEvent.PreparePosition);
					return position <= _lastPosition;
				}

				goto Loop;
			}

			public ReadResp Current => _inner.Current;
		}
	}
}
