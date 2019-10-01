using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using Xunit;
using EventStore.ClientAPI.Common.Utils;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Services;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Projections.Core.Tests.Services.projections_manager {
	public class when_deleting_a_system_projection {
		public static IEnumerable<object[]> TestCases()
			=> typeof(ProjectionNamesBuilder.StandardProjections).GetFields(
					System.Reflection.BindingFlags.Public |
					System.Reflection.BindingFlags.Static |
					System.Reflection.BindingFlags.FlattenHierarchy)
				.Where(x => x.IsLiteral && !x.IsInitOnly)
				.Select(x => new[] {x.GetRawConstantValue()});

		[Theory, MemberData(nameof(TestCases)), Trait("Category", "v8")]
		public void a_projection_deleted_event_is_not_written(string systemProjectionName) {
			using var fixture = new Fixture(systemProjectionName);
			Assert.False(
				fixture.Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Any(x =>
					x.Events[0].EventType == ProjectionEventTypes.ProjectionDeleted &&
					Helper.UTF8NoBom.GetString(x.Events[0].Data) == systemProjectionName));
		}

		class Fixture : TestFixtureWithProjectionCoreAndManagementServices {
			private readonly string _systemProjectionName;

			public Fixture(string projectionName) {
				_systemProjectionName = projectionName;
			}

			protected override bool GivenInitializeSystemProjections() {
				return true;
			}

			protected override void Given() {
				AllWritesSucceed();
				NoOtherStreams();
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
				yield return new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now));
				yield return new SystemMessage.SystemCoreReady();
				yield return
					new ProjectionManagementMessage.Command.Disable(
						new PublishEnvelope(_bus), _systemProjectionName, ProjectionManagementMessage.RunAs.System);
				yield return
					new ProjectionManagementMessage.Command.Delete(
						new PublishEnvelope(_bus), _systemProjectionName,
						ProjectionManagementMessage.RunAs.System, false, false, false);
			}
		}
	}
}
