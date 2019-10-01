using System.Linq;
using System.Security.Principal;
using EventStore.Core.Messages;
using Xunit;

namespace EventStore.Core.Tests.Authentication {
	public class when_handling_multiple_requests_with_the_same_correct_user_name_and_password :
		with_internal_authentication_provider {
		private bool _unauthorized;
		private IPrincipal _authenticatedAs;
		private bool _error;

		protected override void Given() {
			base.Given();
			ExistingEvent("$user-user", "$user", null, "{LoginName:'user', Salt:'drowssap',Hash:'password'}");
		}

		public when_handling_multiple_requests_with_the_same_correct_user_name_and_password() {
			SetUpProvider();

			_internalAuthenticationProvider.Authenticate(
				new TestAuthenticationRequest("user", "password", () => { }, p => { }, () => { }, () => { }));

			Consumer.HandledMessages.Clear();

			_internalAuthenticationProvider.Authenticate(
				new TestAuthenticationRequest(
					"user", "password", () => _unauthorized = true, p => _authenticatedAs = p, () => _error = true,
					() => { }));
		}

		[Fact]
		public void authenticates_user() {
			Assert.False(_unauthorized);
			Assert.False(_error);
			Assert.NotNull(_authenticatedAs);
			Assert.True(_authenticatedAs.IsInRole("user"));
		}

		[Fact]
		public void does_not_publish_any_read_requests() {
			Assert.Equal(0, Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsBackward>().Count());
			Assert.Equal(0, Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Count());
		}
	}
}
