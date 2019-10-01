using EventStore.Core.Cluster.Settings;
using EventStore.Core.TransactionLog.Chunks;
using Xunit;
using System;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Common.VNodeBuilderTests {
	public abstract class SingleNodeScenario : IAsyncLifetime {
		protected VNodeBuilder _builder;
		protected ClusterVNode _node;
		protected ClusterVNodeSettings _settings;
		protected TFChunkDbConfig _dbConfig;

		public virtual void TestFixtureSetUp() {
			_builder = TestVNodeBuilder.AsSingleNode()
				.RunInMemory();
			Given();
			_node = _builder.Build();
			_settings = ((TestVNodeBuilder)_builder).GetSettings();
			_dbConfig = ((TestVNodeBuilder)_builder).GetDbConfig();
			_node.Start();
		}

		public Task TestFixtureTearDown() {
			return _node.Stop().WithTimeout(TimeSpan.FromSeconds(20));
		}

		public abstract void Given();

		public Task InitializeAsync() {
			TestFixtureSetUp();
			return Task.CompletedTask;
		}

		public Task DisposeAsync() {
			return TestFixtureTearDown();
		}
	}

	[Trait("Category", "LongRunning")]
	public abstract class ClusterMemberScenario : IAsyncLifetime {
		protected VNodeBuilder _builder;
		protected ClusterVNode _node;
		protected ClusterVNodeSettings _settings;
		protected TFChunkDbConfig _dbConfig;
		protected int _clusterSize = 3;
		protected int _quorumSize;

		public virtual void TestFixtureSetUp() {
			_builder = TestVNodeBuilder.AsClusterMember(_clusterSize)
				.RunInMemory();
			_quorumSize = _clusterSize / 2 + 1;
			Given();
			_node = _builder.Build();
			_settings = ((TestVNodeBuilder)_builder).GetSettings();
			_dbConfig = ((TestVNodeBuilder)_builder).GetDbConfig();
			_node.Start();
		}

		public virtual Task TestFixtureTearDown() {
			return _node.Stop();
		}

		public abstract void Given();

		public Task InitializeAsync() {
			TestFixtureSetUp();
			return Task.CompletedTask;
		}

		public Task DisposeAsync() {
			return TestFixtureTearDown();
		}
	}
}
