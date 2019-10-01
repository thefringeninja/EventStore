using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.SystemData;
using EventStore.Common.Options;
using EventStore.Core;
using EventStore.Core.Bus;
using EventStore.Core.Tests;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Util;
using EventStore.Projections.Core.Services.Processing;
using Xunit;
using ResolvedEvent = EventStore.ClientAPI.ResolvedEvent;
using EventStore.ClientAPI.Projections;

namespace EventStore.Projections.Core.Tests.ClientAPI {
	[Trait("Category", "ClientAPI")]
	public class specification_with_standard_projections_runnning : SpecificationWithDirectoryPerTestFixture {
		protected MiniNode _node;
		protected IEventStoreConnection _conn;
		protected ProjectionsSubsystem _projections;
		protected UserCredentials _admin = DefaultData.AdminCredentials;
		protected ProjectionsManager _manager;
		protected QueryManager _queryManager;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
#if (!DEBUG)
			//Assert.Ignore("These tests require DEBUG conditional");
#else
			QueueStatsCollector.InitializeIdleDetection();
			await CreateNode();
			_conn = EventStoreConnection.Create(_node.TcpEndPoint);
			await _conn.ConnectAsync();

			_manager = new ProjectionsManager(
				new ConsoleLogger(),
				_node.ExtHttpEndPoint,
				TimeSpan.FromMilliseconds(20000));

			_queryManager = new QueryManager(
				new ConsoleLogger(),
				_node.ExtHttpEndPoint,
				TimeSpan.FromMilliseconds(20000),
				TimeSpan.FromMilliseconds(20000));

			WaitIdle();

			if (GivenStandardProjectionsRunning())
				await EnableStandardProjections();

			QueueStatsCollector.WaitIdle();
			try {
				await Given().WithTimeout(TimeSpan.FromSeconds(10));
			} catch (Exception ex) {
				throw new Exception("Given Failed", ex);
			}

			try {
				await When().WithTimeout(TimeSpan.FromSeconds(10));
			} catch (Exception ex) {
				throw new Exception("When Failed", ex);
			}

#endif
		}

		private Task CreateNode() {
			var projectionWorkerThreadCount = GivenWorkerThreadCount();
			_projections = new ProjectionsSubsystem(projectionWorkerThreadCount, runProjections: ProjectionType.All,
				startStandardProjections: false,
				projectionQueryExpiry: TimeSpan.FromMinutes(Opts.ProjectionsQueryExpiryDefault),
				faultOutOfOrderProjections: Opts.FaultOutOfOrderProjectionsDefault);
			_node = new MiniNode(
				PathName, inMemDb: true, skipInitializeStandardUsersCheck: false,
				subsystems: new ISubsystem[] {_projections});
			return _node.Start();
		}

		protected virtual int GivenWorkerThreadCount() {
			return 1;
		}


		protected async Task EnableStandardProjections() {
			await EnableProjection(ProjectionNamesBuilder.StandardProjections.EventByCategoryStandardProjection);
			await EnableProjection(ProjectionNamesBuilder.StandardProjections.EventByTypeStandardProjection);
			await EnableProjection(ProjectionNamesBuilder.StandardProjections.StreamByCategoryStandardProjection);
			await EnableProjection(ProjectionNamesBuilder.StandardProjections.StreamsStandardProjection);
		}

		protected async Task DisableStandardProjections() {
			await DisableProjection(ProjectionNamesBuilder.StandardProjections.EventByCategoryStandardProjection);
			await DisableProjection(ProjectionNamesBuilder.StandardProjections.EventByTypeStandardProjection);
			await DisableProjection(ProjectionNamesBuilder.StandardProjections.StreamByCategoryStandardProjection);
			await DisableProjection(ProjectionNamesBuilder.StandardProjections.StreamsStandardProjection);
		}

		protected virtual bool GivenStandardProjectionsRunning() {
			return true;
		}

		protected Task EnableProjection(string name) {
			return _manager.EnableAsync(name, _admin);
		}

		protected Task DisableProjection(string name) {
			return _manager.DisableAsync(name, _admin);
		}

		public override async Task TestFixtureTearDown() {
			var all = await _manager.ListAllAsync(_admin);
			if (all.Any(p => p.Name == "Faulted"))
				throw new Exception("Projections faulted while running the test" + "\r\n" + all);
			if (_conn != null)
				_conn.Close();

			if (_node != null)
				await _node.Shutdown();
#if DEBUG
			QueueStatsCollector.DisableIdleDetection();
#endif
			await base.TestFixtureTearDown();
		}

		protected virtual Task When() => Task.CompletedTask;

		protected virtual Task Given() => Task.CompletedTask;

		protected Task PostEvent(string stream, string eventType, string data) {
			return _conn.AppendToStreamAsync(stream, ExpectedVersion.Any, CreateEvent(eventType, data));
		}

		protected Task HardDeleteStream(string stream) {
			return _conn.DeleteStreamAsync(stream, ExpectedVersion.Any, true, _admin);
		}

		protected Task SoftDeleteStream(string stream) {
			return _conn.DeleteStreamAsync(stream, ExpectedVersion.Any, false, _admin);
		}

		protected static EventData CreateEvent(string type, string data) {
			return new EventData(Guid.NewGuid(), type, true, Encoding.UTF8.GetBytes(data), new byte[0]);
		}

		protected static void WaitIdle(int multiplier = 1) {
			QueueStatsCollector.WaitIdle(multiplier: multiplier);
		}

		protected async Task AssertStreamTail(string streamId, params string[] events) {
#if DEBUG
			var result = await _conn.ReadStreamEventsBackwardAsync(streamId, -1, events.Length, true, _admin);
			switch (result.Status) {
				case SliceReadStatus.StreamDeleted:
					throw new Exception($"Stream '{streamId}' is deleted");
					break;
				case SliceReadStatus.StreamNotFound:
					throw new Exception(string.Format("Stream '{0}' does not exist", streamId));
					break;
				case SliceReadStatus.Success:
					var resultEventsReversed = result.Events.Reverse().ToArray();
					if (resultEventsReversed.Length < events.Length)
						DumpFailed("Stream does not contain enough events", streamId, events, result.Events);
					else {
						for (var index = 0; index < events.Length; index++) {
							var parts = events[index].Split(new char[] {':'}, 2);
							var eventType = parts[0];
							var eventData = parts[1];

							if (resultEventsReversed[index].Event.EventType != eventType)
								DumpFailed("Invalid event type", streamId, events, resultEventsReversed);
							else if (resultEventsReversed[index].Event.DebugDataView != eventData)
								DumpFailed("Invalid event body", streamId, events, resultEventsReversed);
						}
					}

					break;
			}
#endif
		}

		protected async Task DumpStream(string streamId) {
#if DEBUG
			var result = await _conn.ReadStreamEventsBackwardAsync(streamId, -1, 100, true, _admin);
			switch (result.Status) {
				case SliceReadStatus.StreamDeleted:
					throw new Exception(string.Format("Stream '{0}' is deleted", streamId));
					break;
				case SliceReadStatus.StreamNotFound:
					throw new Exception(string.Format("Stream '{0}' does not exist", streamId));
					break;
				case SliceReadStatus.Success:
					Dump("Dumping..", streamId, result.Events.Reverse().ToArray());
					break;
			}
#endif
		}

#if DEBUG
		private void DumpFailed(string message, string streamId, string[] events, ResolvedEvent[] resultEvents) {
			var expected = events.Aggregate("", (a, v) => a + ", " + v);
			var actual = resultEvents.Aggregate(
				"", (a, v) => a + ", " + v.Event.EventType + ":" + v.Event.DebugDataView);

			var actualMeta = resultEvents.Aggregate(
				"", (a, v) => a + "\r\n" + v.Event.EventType + ":" + v.Event.DebugMetadataView);


			throw new Exception(
				$"Stream: '{streamId}'\r\n{message}\r\n\r\nExisting events: \r\n{actual}\r\n Expected events: \r\n{expected}\r\n\r\nActual metas:{actualMeta}");
		}

		protected void Dump(string message, string streamId, ResolvedEvent[] resultEvents) {
			var actual = resultEvents.Aggregate(
				"", (a, v) => a + ", " + v.OriginalEvent.EventType + ":" + v.OriginalEvent.DebugDataView);

			var actualMeta = resultEvents.Aggregate(
				"", (a, v) => a + "\r\n" + v.OriginalEvent.EventType + ":" + v.OriginalEvent.DebugMetadataView);


			Debug.WriteLine(
				"Stream: '{0}'\r\n{1}\r\n\r\nExisting events: \r\n{2}\r\n \r\nActual metas:{3}", streamId,
				message, actual, actualMeta);
		}
#endif

		protected async Task PostProjection(string query) {
			await _manager.CreateContinuousAsync("test-projection", query, _admin);
			WaitIdle();
		}

		protected async Task PostQuery(string query) {
			await _manager.CreateTransientAsync("query", query, _admin);
			WaitIdle();
		}
	}
}
