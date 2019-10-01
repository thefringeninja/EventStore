﻿using EventStore.ClientAPI;
using EventStore.Core.Tests.ClientAPI.Helpers;
using EventStore.Core.Tests.Helpers;
using Xunit;

namespace EventStore.Core.Tests.ClientAPI.Embedded {
	[Trait("Category", "LongRunning")]
	public class update_existing_persistent_subscription : ClientAPI.update_existing_persistent_subscription {
		protected override IEventStoreConnection BuildConnection(MiniNode node) {
			return EmbeddedTestConnection.To(node);
		}
	}

	[Trait("Category", "LongRunning")]
	public class update_existing_persistent_subscription_with_subscribers :
		ClientAPI.update_existing_persistent_subscription_with_subscribers {
		protected override IEventStoreConnection BuildConnection(MiniNode node) {
			return EmbeddedTestConnection.To(node);
		}
	}

	[Trait("Category", "LongRunning")]
	public class update_non_existing_persistent_subscription : ClientAPI.update_non_existing_persistent_subscription {
		protected override IEventStoreConnection BuildConnection(MiniNode node) {
			return EmbeddedTestConnection.To(node);
		}
	}

	[Trait("Category", "LongRunning")]
	public class update_existing_persistent_subscription_without_permissions :
		ClientAPI.update_existing_persistent_subscription_without_permissions {
		protected override IEventStoreConnection BuildConnection(MiniNode node) {
			return EmbeddedTestConnection.To(node);
		}
	}
}
