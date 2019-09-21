﻿using System;
using System.Net;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Core.Bus;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;
using System.IO;
using System.Threading.Tasks;

namespace EventStore.Core.Tests.Integration {
	public class specification_with_a_single_node : SpecificationWithDirectoryPerTestFixture {
		protected MiniNode _node;

		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName, dbPath: Path.Combine(PathName, "db"), inMemDb: false);

			BeforeNodeStarts();

			await _node.Start();

			await Given();
		}

		protected virtual void BeforeNodeStarts() {
		}

		protected virtual Task Given() => Task.CompletedTask;

		protected void ShutdownNode() {
			_node.Shutdown(keepDb: true, keepPorts: true);
			_node = null;
		}

		protected Task StartNode() {
			if (_node == null)
				_node = new MiniNode(PathName, dbPath: Path.Combine(PathName, "db"), inMemDb: false);

			BeforeNodeStarts();

			return _node.Start();
		}

		[OneTimeTearDown]
		public override Task TestFixtureTearDown() {
			_node.Shutdown();
			_node = null;
			return base.TestFixtureTearDown();
		}
	}
}
