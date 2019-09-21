﻿using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;

namespace EventStore.Core.Tests.ClientAPI {
	[TestFixture, Category("ClientAPI"), Category("LongRunning")]
	public class deleting_stream : SpecificationWithDirectoryPerTestFixture {
		private MiniNode _node;

		[OneTimeSetUp]
		public override async Task TestFixtureSetUp() {
			await base.TestFixtureSetUp();
			_node = new MiniNode(PathName);
			await _node.Start();
		}

		[OneTimeTearDown]
		public override Task TestFixtureTearDown() {
			_node.Shutdown();
			return base.TestFixtureTearDown();
		}

		virtual protected IEventStoreConnection BuildConnection(MiniNode node) {
			return TestConnection.Create(node.TcpEndPoint);
		}

		[Test]
		[Category("Network")]
		public void which_doesnt_exists_should_success_when_passed_empty_stream_expected_version() {
			const string stream = "which_already_exists_should_success_when_passed_empty_stream_expected_version";
			using (var connection = BuildConnection(_node)) {
				connection.ConnectAsync().Wait();
				var delete = connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);
				Assert.DoesNotThrow(delete.Wait);
			}
		}

		[Test]
		[Category("Network")]
		public void which_doesnt_exists_should_success_when_passed_any_for_expected_version() {
			const string stream = "which_already_exists_should_success_when_passed_any_for_expected_version";
			using (var connection = BuildConnection(_node)) {
				connection.ConnectAsync().Wait();

				var delete = connection.DeleteStreamAsync(stream, ExpectedVersion.Any, hardDelete: true);
				Assert.DoesNotThrow(delete.Wait);
			}
		}

		[Test]
		[Category("Network")]
		public void with_invalid_expected_version_should_fail() {
			const string stream = "with_invalid_expected_version_should_fail";
			using (var connection = BuildConnection(_node)) {
				connection.ConnectAsync().Wait();

				var delete = connection.DeleteStreamAsync(stream, 1, hardDelete: true);
				Assert.That(() => delete.Wait(),
					Throws.Exception.TypeOf<AggregateException>().With.InnerException
						.TypeOf<WrongExpectedVersionException>());
			}
		}

		public void should_return_log_position_when_writing() {
			const string stream = "delete_should_return_log_position_when_writing";
			using (var connection = BuildConnection(_node)) {
				connection.ConnectAsync().Wait();

				var result = connection
					.AppendToStreamAsync(stream, ExpectedVersion.NoStream, TestEvent.NewTestEvent()).Result;
				var delete = connection.DeleteStreamAsync(stream, 1, hardDelete: true).Result;

				Assert.IsTrue(0 < result.LogPosition.PreparePosition);
				Assert.IsTrue(0 < result.LogPosition.CommitPosition);
			}
		}

		[Test]
		[Category("Network")]
		public void which_was_already_deleted_should_fail() {
			const string stream = "which_was_allready_deleted_should_fail";
			using (var connection = BuildConnection(_node)) {
				connection.ConnectAsync().Wait();

				var delete = connection.DeleteStreamAsync(stream, ExpectedVersion.NoStream, hardDelete: true);
				Assert.DoesNotThrow(delete.Wait);

				var secondDelete = connection.DeleteStreamAsync(stream, ExpectedVersion.Any, hardDelete: true);
				Assert.That(() => secondDelete.Wait(),
					Throws.Exception.TypeOf<AggregateException>().With.InnerException.TypeOf<StreamDeletedException>());
			}
		}
	}
}
