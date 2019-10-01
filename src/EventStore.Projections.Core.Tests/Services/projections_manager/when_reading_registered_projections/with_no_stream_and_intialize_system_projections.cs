using System;
using System.Linq;
using EventStore.Core.Messages;
using Xunit;
using EventStore.Projections.Core.Services.Processing;
using System.Collections.Generic;
using EventStore.Common.Utils;
using EventStore.Projections.Core.Services;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Projections.Core.Tests.Services.projections_manager.when_reading_registered_projections {
	public class with_no_stream_and_intialize_system_projections {
		public static IEnumerable<object[]> TestCases()
			=> typeof(ProjectionNamesBuilder.StandardProjections).GetFields(
					System.Reflection.BindingFlags.Public |
					System.Reflection.BindingFlags.Static |
					System.Reflection.BindingFlags.FlattenHierarchy)
				.Where(x => x.IsLiteral && !x.IsInitOnly)
				.Select(x => new[] {x.GetRawConstantValue()});

		[Theory, MemberData(nameof(TestCases))]
		public void it_should_write_the_projections_initialized_event(string systemProjectionName) {
			using var fixture = new Fixture(systemProjectionName);
			Assert.Equal(1, fixture.Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count(x =>
				x.EventStreamId == ProjectionNamesBuilder.ProjectionsRegistrationStream &&
				x.Events[0].EventType == ProjectionEventTypes.ProjectionsInitialized));
		}

		[Theory, MemberData(nameof(TestCases))]
		public void it_should_write_the_system_projection_created_event(string systemProjectionName) {
			using var fixture = new Fixture(systemProjectionName);
			Assert.Equal(1, fixture.Consumer.HandledMessages.OfType<ClientMessage.WriteEvents>().Count(x =>
				x.EventStreamId == ProjectionNamesBuilder.ProjectionsRegistrationStream &&
				x.Events[0].EventType == ProjectionEventTypes.ProjectionCreated &&
				Helper.UTF8NoBom.GetString(x.Events[0].Data) == systemProjectionName));
		}

		class Fixture : TestFixtureWithProjectionCoreAndManagementServices {
			private readonly string _systemProjectionName;

			public Fixture(string projectionName) {
				_systemProjectionName = projectionName;
			}

			protected override void Given() {
				AllWritesSucceed();
				NoStream(ProjectionNamesBuilder.ProjectionsRegistrationStream);
				NoOtherStreams();
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new SystemMessage.BecomeMaster(Guid.NewGuid());
				yield return new SystemMessage.EpochWritten(new EpochRecord(0L, 0, Guid.NewGuid(), 0L, DateTime.Now));
				yield return new SystemMessage.SystemCoreReady();
			}

			protected override bool GivenInitializeSystemProjections() {
				return true;
			}
		}
	}
}
