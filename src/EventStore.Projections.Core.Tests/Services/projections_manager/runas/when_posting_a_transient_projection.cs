using System;
using System.Collections.Generic;
using System.Security.Principal;
using EventStore.Core.Authentication;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using Xunit;
using System.Linq;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.runas {
	namespace when_posting_a_transient_projection {
		public class authenticated : TestFixtureWithProjectionCoreAndManagementServices {
			private string _projectionName;
			private OpenGenericPrincipal _testUserPrincipal;

			private string _projectionBody = @"fromAll().when({$any:function(s,e){return s;}});";

			protected override void Given() {
				_projectionName = "test-projection";
				_projectionBody = @"fromAll().when({$any:function(s,e){return s;}});";
				_testUserPrincipal = new OpenGenericPrincipal(
					new GenericIdentity("test-user"), new[] {"test-role1", "test-role2"});

				AllWritesSucceed();
				NoOtherStreams();
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
				yield return new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now));
				yield return new SystemMessage.SystemCoreReady();
				yield return
					new ProjectionManagementMessage.Command.Post(
						new PublishEnvelope(GetInputQueue()), ProjectionMode.Transient, _projectionName,
						new ProjectionManagementMessage.RunAs(_testUserPrincipal), "JS", _projectionBody, enabled: true,
						checkpointsEnabled: true, emitEnabled: true, trackEmittedStreams: true, enableRunAs: true);
			}

			[Fact(Skip = "ignored")]
			public void anonymous_cannot_retrieve_projection_query() {
				GetInputQueue()
					.Publish(
						new ProjectionManagementMessage.Command.GetQuery(
							Envelope, _projectionName, ProjectionManagementMessage.RunAs.Anonymous));
				Queue.Process();

				Assert.True(HandledMessages.OfType<ProjectionManagementMessage.NotAuthorized>().Any());
			}

			[Fact]
			public void projection_owner_can_retrieve_projection_query() {
				GetInputQueue()
					.Publish(
						new ProjectionManagementMessage.Command.GetQuery(
							Envelope, _projectionName, new ProjectionManagementMessage.RunAs(_testUserPrincipal)));
				Queue.Process();

				var query = HandledMessages.OfType<ProjectionManagementMessage.ProjectionQuery>().FirstOrDefault();
				Assert.NotNull(query);
				Assert.Equal(_projectionBody, query.Query);
			}
		}

		public class anonymous : TestFixtureWithProjectionCoreAndManagementServices {
			private string _projectionName;

			private string _projectionBody = @"fromAll().when({$any:function(s,e){return s;}});";

			protected override void Given() {
				_projectionName = "test-projection";
				_projectionBody = @"fromAll().when({$any:function(s,e){return s;}});";

				AllWritesSucceed();
				NoOtherStreams();
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
				yield return new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now));
				yield return new SystemMessage.SystemCoreReady();
				yield return
					new ProjectionManagementMessage.Command.Post(
						new PublishEnvelope(GetInputQueue()), ProjectionMode.Continuous, _projectionName,
						ProjectionManagementMessage.RunAs.Anonymous, "JS", _projectionBody, enabled: true,
						checkpointsEnabled: true, emitEnabled: true, trackEmittedStreams: true, enableRunAs: true);
			}

			[Fact]
			public void replies_with_not_authorized() {
				Assert.True(HandledMessages.OfType<ProjectionManagementMessage.NotAuthorized>().Any());
			}
		}
	}
}
