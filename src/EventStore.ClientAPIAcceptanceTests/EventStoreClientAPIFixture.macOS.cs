using System.Threading.Tasks;
using Xunit;

namespace EventStore.ClientAPI.Tests {
	public partial class EventStoreClientAPIFixture : IAsyncLifetime {
		private readonly EventStoreDockerContainer _container;

		public EventStoreClientAPIFixture() {
			_container = new EventStoreDockerContainer(ExternalPort, ExternalSecurePort);
		}

		public Task InitializeAsync() => _container.TryStart().AsTask();

		public Task DisposeAsync() => _container.DisposeAsync().AsTask();
	}
}
