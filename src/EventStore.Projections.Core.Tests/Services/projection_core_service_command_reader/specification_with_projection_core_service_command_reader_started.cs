using System.Collections.Generic;
using System.Linq;
using EventStore.ClientAPI.Common.Utils;
using EventStore.Core.Data;
using EventStore.Projections.Core.Messages;
using Newtonsoft.Json.Linq;
using Xunit;
using EventStore.Projections.Core.Services.Processing;
using System;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service_command_reader {
	public abstract class specification_with_projection_core_service_command_reader_started
		: specification_with_projection_core_service_command_reader {
		protected string _serviceId;
		protected Guid _uniqueStreamId;

		protected override IEnumerable<WhenStep> PreWhen() {
			_uniqueStreamId = Guid.NewGuid();
			var startCore = new ProjectionCoreServiceMessage.StartCore(_uniqueStreamId);
			var startReader = CreateWriteEvent(ProjectionNamesBuilder.BuildControlStreamName(_uniqueStreamId),
				"$response-reader-started", "{}");
			yield return new WhenStep(startCore, startReader);
			Assert.True(_streams.TryGetValue("$projections-$master", out var stream));
			Assert.NotNull(stream);
			var lastEvent = stream.Last();
			var parsed = lastEvent.Data.ParseJson<JObject>();
			_serviceId = (string)((JValue)parsed.GetValue("id")).Value;
			Assert.False(string.IsNullOrEmpty(_serviceId));
		}
	}
}
