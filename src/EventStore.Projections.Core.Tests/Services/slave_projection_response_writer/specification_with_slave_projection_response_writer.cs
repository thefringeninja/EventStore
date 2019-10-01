using System;
using System.Collections.Generic;
using EventStore.ClientAPI.Common.Utils;
using EventStore.Projections.Core.Services.Management;
using Xunit;

namespace EventStore.Projections.Core.Tests.Services.slave_projection_response_writer {
		public abstract class specification_with_slave_projection_response_writer {
		protected SlaveProjectionResponseWriter _sut;
		protected List<Tuple<string, Guid, object>> _publishedResponses;
		private IMultiStreamMessageWriter _writer;

		public specification_with_slave_projection_response_writer() {
			_publishedResponses = new List<Tuple<string, Guid, object>>();
			_writer = new FakeWriter(this);
			_sut = new SlaveProjectionResponseWriter(_writer);
			Given();
			When();
		}

		protected T AssertParsedSingleResponse<T>(string response, Guid workerId) {
			Assert.Equal(1, _publishedResponses.Count);
			Assert.Equal(response, _publishedResponses[0].Item1);
			Assert.Equal(workerId, _publishedResponses[0].Item2);
			Assert.IsType<T>(_publishedResponses[0].Item3);
			var source = (T)_publishedResponses[0].Item3;
			var serialized = source.ToJson();
			var parsed = serialized.ParseJson<T>();
			return parsed;
		}

		protected virtual void Given() {
		}

		protected abstract void When();

		public class FakeWriter : IMultiStreamMessageWriter {
			private readonly specification_with_slave_projection_response_writer _container;

			public FakeWriter(specification_with_slave_projection_response_writer container) {
				_container = container;
			}

			public void PublishResponse(string command, Guid workerId, object body) {
				_container.PublishResponse(command, workerId, body);
			}

			public void Reset() {
			}
		}

		private void PublishResponse(string command, Guid workerId, object body) {
			_publishedResponses.Add(Tuple.Create(command, workerId, body));
		}
	}
}
