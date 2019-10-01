using System;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Projections;
using EventStore.ClientAPI.Common.Log;
using EventStore.ClientAPI.SystemData;
using EventStore.Common.Options;
using EventStore.Core;
using EventStore.Core.Tests;
using EventStore.Core.Tests.Helpers;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Services;
using EventStore.Core.Util;

namespace EventStore.Projections.Core.Tests.ClientAPI.projectionsManager {
	public abstract class SpecificationWithNodeAndProjectionsManager : SpecificationWithDirectoryPerTestFixture {
		protected MiniNode _node;
		protected ProjectionsManager _projManager;
		protected IEventStoreConnection _connection;
		protected UserCredentials _credentials;
		protected TimeSpan _timeout;
		protected string _tag;

		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_credentials = new UserCredentials(SystemUsers.Admin, SystemUsers.DefaultAdminPassword);
			_timeout = TimeSpan.FromSeconds(10);
			// Check if a node is running in ProjectionsManagerTestSuiteMarkerBase
			_tag = "_1";

			_node = CreateNode();
			await _node.Start();

			_connection = TestConnection.Create(_node.TcpEndPoint);
			await _connection.ConnectAsync();

			_projManager = new ProjectionsManager(new ConsoleLogger(), _node.ExtHttpEndPoint, _timeout);
			try {
				await Given().WithTimeout(_timeout);
			} catch (Exception ex) {
				throw new Exception("Given Failed", ex);
			}

			try {
				await When().WithTimeout(_timeout);
			} catch (Exception ex) {
				throw new Exception("When Failed", ex);
			}
		}

		public override async Task TestFixtureTearDown() {
			_connection.Close();
			await _node.Shutdown();

			await base.TestFixtureTearDown();
		}

		public abstract Task Given();
		public abstract Task When();

		protected MiniNode CreateNode() {
			var projections = new ProjectionsSubsystem(1, runProjections: ProjectionType.All,
				startStandardProjections: false,
				projectionQueryExpiry: TimeSpan.FromMinutes(Opts.ProjectionsQueryExpiryDefault),
				faultOutOfOrderProjections: Opts.FaultOutOfOrderProjectionsDefault);
			return new MiniNode(
				PathName, inMemDb: true, skipInitializeStandardUsersCheck: false,
				subsystems: new ISubsystem[] {projections});
		}

		protected EventData CreateEvent(string eventType, string data) {
			return new EventData(Guid.NewGuid(), eventType, true, Encoding.UTF8.GetBytes(data), null);
		}

		protected Task PostEvent(string stream, string eventType, string data) {
			return _connection.AppendToStreamAsync(stream, ExpectedVersion.Any, new[] {CreateEvent(eventType, data)});
		}

		protected Task CreateOneTimeProjection() {
			var query = CreateStandardQuery(Guid.NewGuid().ToString());
			return _projManager.CreateOneTimeAsync(query, _credentials);
		}

		protected Task CreateContinuousProjection(string projectionName) {
			var query = CreateStandardQuery(Guid.NewGuid().ToString());
			return _projManager.CreateContinuousAsync(projectionName, query, _credentials);
		}

		protected string CreateStandardQuery(string stream) {
			return @"fromStream(""" + stream + @""")
                .when({
                    ""$any"":function(s,e) {
                        s.count = 1;
                        return s;
                    }
            });";
		}

		protected string CreateEmittingQuery(string stream, string emittingStream) {
			return @"fromStream(""" + stream + @""")
                .when({
                    ""$any"":function(s,e) {
                        emit(""" + emittingStream + @""", ""emittedEvent"", e);
                    } 
                });";
		}
	}
}
