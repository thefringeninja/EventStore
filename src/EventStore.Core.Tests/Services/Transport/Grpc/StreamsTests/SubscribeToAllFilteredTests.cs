using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Client.Shared;
using EventStore.Client.Streams;
using EventStore.Core.Services.Transport.Grpc;
using Grpc.Core;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Transport.Grpc.StreamsTests {
	[TestFixture]
	public class SubscribeToAllFilteredTests {
		[TestFixtureSource(nameof(TestCases))]
		public class when_subscribing_to_all_with_a_filter : GrpcSpecification.Read {
			private int CheckpointCount => Positions.Count();

			private IEnumerable<Position> Positions => ReadResponses
				.Where(response => response.ContentCase == ReadResp.ContentOneofCase.Checkpoint)
				.Select(response =>
					new Position(response.Checkpoint.CommitPosition, response.Checkpoint.PreparePosition));

			private readonly uint _maxSearchWindow;
			private readonly int _filteredEventCount;
			private readonly uint _checkpointIntervalMultiplier;
			private readonly uint _checkpointInterval;

			private long _expected;

			protected override (string userName, string password) DefaultCredentials => AdminCredentials;

			public static IEnumerable<object[]> TestCases() {
				var checkpointIntervalMultipliers = new uint[] {2, 4, 8};

				var maxSearchWindows = new uint[] {1, 32, 64};

				var filteredEventCount = checkpointIntervalMultipliers.Max() * maxSearchWindows.Max();

				return from checkpointInterval in checkpointIntervalMultipliers
					from maxSearchWindow in maxSearchWindows
					select new object[] {
						checkpointInterval,
						maxSearchWindow,
						(int)filteredEventCount
					};
			}

			public when_subscribing_to_all_with_a_filter(uint checkpointIntervalMultiplier, uint maxSearchWindow,
				int filteredEventCount) {
				_maxSearchWindow = maxSearchWindow;
				_checkpointIntervalMultiplier = checkpointIntervalMultiplier;
				_checkpointInterval = checkpointIntervalMultiplier * maxSearchWindow;
				_filteredEventCount = filteredEventCount;
			}

			protected override IAsyncEnumerator<ReadResp> GetEnumerator(IAsyncEnumerable<ReadResp> enumerable) =>
				new SubscriptionEnumerator(enumerable, PositionOfLastWrite);

			protected override ReadReq ReadRequest => new ReadReq {
				Options = new ReadReq.Types.Options {
					All = new ReadReq.Types.Options.Types.AllOptions {
						Start = new Empty()
					},
					Subscription = new ReadReq.Types.Options.Types.SubscriptionOptions(),
					Filter = new ReadReq.Types.Options.Types.FilterOptions {
						CheckpointIntervalMultiplier = _checkpointIntervalMultiplier,
						Max = _maxSearchWindow,
						StreamIdentifier = new ReadReq.Types.Options.Types.FilterOptions.Types.Expression {
							Prefix = {StreamName}
						}
					},
					ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Forwards,
					ResolveLinks = false,
					UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
						Structured = new Empty()
					},
				}
			};

			protected override async Task Given() {
				await AppendToStream("filtered-out", CreateEvents(_filteredEventCount));

				await base.Given();

				var skippedEventCount = await GetSkippedEventCount();
				_expected = (int)Math.Round(skippedEventCount / (double)_checkpointInterval);

				async Task<int> GetSkippedEventCount() {
					using var call = StreamsClient.Read(new ReadReq {
						Options = new ReadReq.Types.Options {
							All = new ReadReq.Types.Options.Types.AllOptions {
								Start = new Empty()
							},
							Count = ulong.MaxValue,
							NoFilter = new Empty(),
							ReadDirection = ReadReq.Types.Options.Types.ReadDirection.Forwards,
							ResolveLinks = false,
							UuidOption = new ReadReq.Types.Options.Types.UUIDOption {
								Structured = new Empty()
							},
						}
					}, GetCallOptions());

					return await call.ResponseStream
						.ReadAllAsync()
						.CountAsync(response => response.ContentCase == ReadResp.ContentOneofCase.Event &&
						                        response.Event.OriginalEvent.StreamIdentifier != StreamName &&
						                        PositionOfLastWrite >= new Position(response.Event.OriginalEvent.CommitPosition,
							                        response.Event.OriginalEvent.PreparePosition));
				}
			}

			[Test]
			public void receives_the_correct_number_of_checkpoints() {
				Assert.AreEqual(_expected, CheckpointCount);
			}

			[Test]
			public void no_duplicate_checkpoints_received() {
				Assert.AreEqual(Positions.Distinct().Count(), Positions.Count());
			}

			[Test]
			public void subscription_confirmed() {
				Assert.AreEqual(ReadResp.ContentOneofCase.Confirmation, ReadResponses[0].ContentCase);
				Assert.NotNull(ReadResponses[0].Confirmation.SubscriptionId);
			}
		}
	}
}
