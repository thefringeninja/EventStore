using System.Linq;
using System.Security.Principal;
using EventStore.Core.Messages;
using Xunit;

namespace EventStore.Core.Tests.Authentication {
	public class when_handling_multiple_requests_with_reset_password_cache_in_between :
		with_internal_authentication_provider {
		private bool _unauthorized;
		private IPrincipal _authenticatedAs;
		private bool _error;

		public when_handling_multiple_requests_with_reset_password_cache_in_between() {
			SetUpProvider();

			_internalAuthenticationProvider.Authenticate(
				new TestAuthenticationRequest("user", "password", () => { }, p => { }, () => { }, () => { }));
			_internalAuthenticationProvider.Handle(
				new InternalAuthenticationProviderMessages.ResetPasswordCache("user"));
			Consumer.HandledMessages.Clear();

			_internalAuthenticationProvider.Authenticate(
				new TestAuthenticationRequest(
					"user", "password", () => _unauthorized = true, p => _authenticatedAs = p, () => _error = true,
					() => { }));
		}

		protected override void Given() {
			base.Given();
			ExistingEvent("$user-user", "$user", null, "{LoginName:'user', Salt:'drowssap',Hash:'password'}");
		}

		[Fact]
		public void authenticates_user() {
			Assert.False(_unauthorized);
			Assert.False(_error);
			Assert.NotNull(_authenticatedAs);
			Assert.True(_authenticatedAs.IsInRole("user"));
		}

		[Fact]
		public void publishes_some_read_requests() {
			Assert.True(
				Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsBackward>().Count()
				+ Consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Count() > 0);
		}
	}
}
