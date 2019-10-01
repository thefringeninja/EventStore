using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using EventStore.Core.Authentication;
using EventStore.Core.Messages;
using EventStore.Core.Services;
using EventStore.Core.Services.UserManagement;
using EventStore.Core.Tests.Authentication;
using EventStore.Core.Tests.Helpers;
using Xunit;
using EventStore.ClientAPI.Common.Utils;
using Newtonsoft.Json.Linq;

namespace EventStore.Core.Tests.Services.UserManagementService {
	public static class user_management_service {
		public class TestFixtureWithUserManagementService : TestFixtureWithExistingEvents {
			protected Core.Services.UserManagement.UserManagementService _users;
			protected readonly IPrincipal _ordinaryUser = new OpenGenericPrincipal("user1", "role1");

			protected override void Given() {
				base.Given();
				NoStream("$user-user1");
				NoStream("$user-user2");
				NoStream("$user-user3");
				NoOtherStreams();
				AllWritesSucceed();

				_users = new Core.Services.UserManagement.UserManagementService(
					_bus, _ioDispatcher, new StubPasswordHashAlgorithm(), skipInitializeStandardUsersCheck: true);

				_bus.Subscribe<UserManagementMessage.Get>(_users);
				_bus.Subscribe<UserManagementMessage.GetAll>(_users);
				_bus.Subscribe<UserManagementMessage.Create>(_users);
				_bus.Subscribe<UserManagementMessage.Update>(_users);
				_bus.Subscribe<UserManagementMessage.Enable>(_users);
				_bus.Subscribe<UserManagementMessage.Disable>(_users);
				_bus.Subscribe<UserManagementMessage.ResetPassword>(_users);
				_bus.Subscribe<UserManagementMessage.ChangePassword>(_users);
				_bus.Subscribe<UserManagementMessage.Delete>(_users);
				_bus.Subscribe<SystemMessage.BecomeMaster>(_users);
			}

			protected override ManualQueue GiveInputQueue() {
				return new ManualQueue(_bus, _timeProvider);
			}

			public TestFixtureWithUserManagementService() {
				WhenLoop(GivenCommands());
				Queue.Process();
				HandledMessages.Clear();
				WhenLoop();
			}

			protected virtual IEnumerable<WhenStep> GivenCommands() {
				yield break;
			}

			protected ClientMessage.WriteEvents[] HandledPasswordChangedNotificationWrites() {
				return HandledMessages.OfType<ClientMessage.WriteEvents>()
					.Where(
						v =>
							v.EventStreamId
							== Core.Services.UserManagement.UserManagementService
								.UserPasswordNotificationsStreamId).ToArray();
			}

			protected ClientMessage.WriteEvents[] HandledPasswordChangedNotificationMetaStreamWrites() {
				return
					HandledMessages.OfType<ClientMessage.WriteEvents>()
						.Where(
							v =>
								v.EventStreamId
								== SystemStreams.MetastreamOf(
									Core.Services.UserManagement.UserManagementService
										.UserPasswordNotificationsStreamId))
						.ToArray();
			}
		}

		public class when_creating_a_user : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			[Fact]
			public void replies_success() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
			}

			[Fact]
			public void reply_has_the_correct_login_name() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void creates_an_enabled_user_account_with_correct_details() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal("John Doe", user.Data.FullName);
				Assert.Equal("user1", user.Data.LoginName);
				Assert.NotNull(user.Data.Groups);
				Assert.True(user.Data.Groups.Any(v => v == "admin"));
				Assert.True(user.Data.Groups.Any(v => v == "other"));
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void creates_an_enabled_user_account_with_the_correct_password() {
				HandledMessages.Clear();
				_users.Handle(
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "Johny123!", "new-password"));
				Queue.Process();
				var updateResult = HandledMessages.OfType<UserManagementMessage.UpdateResult>().Last();
				Assert.NotNull(updateResult);
				Assert.True(updateResult.Success);
			}
		}

		public class when_ordinary_user_attempts_to_create_a_user : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.Create(
						Envelope, _ordinaryUser, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			[Fact]
			public void replies_unauthorized() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.False(updateResults[0].Success);
				Assert.Equal(UserManagementMessage.Error.Unauthorized, updateResults[0].Error);
			}

			[Fact]
			public void does_not_create_a_user_account() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.False(user.Success);
				Assert.Equal(UserManagementMessage.Error.NotFound, user.Error);
			}
		}

		public class when_creating_an_already_existing_user_account : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "Existing John", new[] {"admin", "other"},
						"existing!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"bad"}, "Johny123!");
			}

			[Fact]
			public void replies_conflict() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.False(updateResults[0].Success);
				Assert.Equal(UserManagementMessage.Error.Conflict, updateResults[0].Error);
			}

			[Fact]
			public void does_not_override_user_details() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal("Existing John", user.Data.FullName);
				Assert.Equal("user1", user.Data.LoginName);
				Assert.NotNull(user.Data.Groups);
				Assert.True(user.Data.Groups.Any(v => v == "admin"));
				Assert.True(user.Data.Groups.Any(v => v == "other"));
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void does_not_override_user_password() {
				HandledMessages.Clear();
				_users.Handle(
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "existing!", "new-password"));
				Queue.Process();
				var updateResult = HandledMessages.OfType<UserManagementMessage.UpdateResult>().Last();
				Assert.NotNull(updateResult);
				Assert.True(updateResult.Success);
			}
		}

		public class when_updating_user_details : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.Update(
						Envelope, SystemAccount.Principal, "user1", "Doe John", new[] {"good"});
			}

			[Fact]
			public void replies_success() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
			}

			[Fact]
			public void reply_has_the_correct_login_name() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void updates_details() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal("Doe John", user.Data.FullName);
				Assert.Equal("user1", user.Data.LoginName);
				Assert.NotNull(user.Data.Groups);
				Assert.True(user.Data.Groups.All(v => v != "admin"));
				Assert.True(user.Data.Groups.All(v => v != "other"));
				Assert.True(user.Data.Groups.Any(v => v == "good"));
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void does_not_update_enabled() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void does_not_change_password() {
				HandledMessages.Clear();
				_users.Handle(
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "Johny123!", "new-password"));
				Queue.Process();
				var updateResult = HandledMessages.OfType<UserManagementMessage.UpdateResult>().Last();
				Assert.NotNull(updateResult);
				Assert.True(updateResult.Success);
			}
		}

		public class when_ordinary_user_attempts_to_update_its_own_details : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.Update(Envelope, _ordinaryUser, "user1", "Doe John", new[] {"good"});
			}

			[Fact]
			public void replies_unauthorized() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.False(updateResults[0].Success);
				Assert.Equal(UserManagementMessage.Error.Unauthorized, updateResults[0].Error);
			}

			[Fact]
			public void details_are_not_changed() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal("John Doe", user.Data.FullName);
				Assert.Equal("user1", user.Data.LoginName);
				Assert.NotNull(user.Data.Groups);
				Assert.True(user.Data.Groups.Any(v => v == "admin"));
				Assert.True(user.Data.Groups.Any(v => v == "other"));
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void does_not_update_enabled() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void does_not_change_password() {
				HandledMessages.Clear();
				_users.Handle(
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "Johny123!", "new-password"));
				Queue.Process();
				var updateResult = HandledMessages.OfType<UserManagementMessage.UpdateResult>().Last();
				Assert.NotNull(updateResult);
				Assert.True(updateResult.Success);
			}
		}

		public class when_updating_non_existing_user_details : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield break;
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.Update(
						Envelope, SystemAccount.Principal, "user1", "Doe John", new[] {"admin", "other"});
			}

			[Fact]
			public void replies_not_found() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.False(updateResults[0].Success);
				Assert.Equal(UserManagementMessage.Error.NotFound, updateResults[0].Error);
			}

			[Fact]
			public void reply_has_the_correct_login_name() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void does_not_create_a_user_account() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.False(user.Success);
				Assert.Equal(UserManagementMessage.Error.NotFound, user.Error);
				Assert.Null(user.Data);
			}
		}

		public class when_updating_a_disabled_user_account_details : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				var replyTo = Envelope;
				yield return
					new UserManagementMessage.Create(
						replyTo, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
				yield return new UserManagementMessage.Disable(replyTo, SystemAccount.Principal, "user1");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.Update(
						Envelope, SystemAccount.Principal, "user1", "Doe John", new[] {"good"});
			}

			[Fact]
			public void replies_success() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
			}

			[Fact]
			public void does_not_update_enabled() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal(true, user.Data.Disabled);
			}
		}

		public class when_disabling_an_enabled_user_account : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new UserManagementMessage.Disable(Envelope, SystemAccount.Principal, "user1");
			}

			[Fact]
			public void replies_success_with_correct_login_name_set() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void disables_user_account() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal(true, user.Data.Disabled);
			}

			[Fact]
			public void writes_password_changed_event() {
				var writePasswordChanged = HandledPasswordChangedNotificationWrites();
				Assert.Equal(1, writePasswordChanged.Length);
			}
		}

		public class when_an_ordinary_user_attempts_to_disable_a_user_account : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new UserManagementMessage.Disable(Envelope, _ordinaryUser, "user1");
			}

			[Fact]
			public void replies_unauthorized() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.False(updateResults[0].Success);
				Assert.Equal(UserManagementMessage.Error.Unauthorized, updateResults[0].Error);
			}

			[Fact]
			public void user_account_is_not_disabled() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.False(user.Data.Disabled);
			}
		}

		public class when_disabling_a_disabled_user_account : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				var replyTo = Envelope;
				yield return
					new UserManagementMessage.Create(
						replyTo, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
				yield return new UserManagementMessage.Disable(replyTo, SystemAccount.Principal, "user1");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new UserManagementMessage.Disable(Envelope, SystemAccount.Principal, "user1");
			}

			[Fact]
			public void replies_success_with_correct_login_name_set() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void keeps_disabled() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal(true, user.Data.Disabled);
			}
		}

		public class when_enabling_a_disabled_user_account : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				var replyTo = Envelope;
				yield return
					new UserManagementMessage.Create(
						replyTo, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
				yield return new UserManagementMessage.Disable(replyTo, SystemAccount.Principal, "user1");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new UserManagementMessage.Enable(Envelope, SystemAccount.Principal, "user1");
			}

			[Fact]
			public void replies_success_with_correct_login_name_set() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void enables_user_account() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.False(user.Data.Disabled);
			}
		}

		public class when_an_ordinary_user_attempts_to_enable_a_user_account : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				var replyTo = Envelope;
				yield return
					new UserManagementMessage.Create(
						replyTo, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
				yield return new UserManagementMessage.Disable(replyTo, SystemAccount.Principal, "user1");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new UserManagementMessage.Enable(Envelope, _ordinaryUser, "user1");
			}

			[Fact]
			public void replies_unauthorized() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.False(updateResults[0].Success);
				Assert.Equal(UserManagementMessage.Error.Unauthorized, updateResults[0].Error);
			}

			[Fact]
			public void user_account_is_not_enabled() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal(true, user.Data.Disabled);
			}
		}

		public class when_enabling_an_enabled_user_account : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new UserManagementMessage.Enable(Envelope, SystemAccount.Principal, "user1");
			}

			[Fact]
			public void replies_success_with_correct_login_name_set() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void keeps_enabled() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.False(user.Data.Disabled);
			}
		}

		public class when_resetting_the_password : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.ResetPassword(Envelope, SystemAccount.Principal, "user1", "new-password");
			}

			[Fact]
			public void replies_success() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
			}

			[Fact]
			public void reply_has_the_correct_login_name() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void does_not_update_details() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal("John Doe", user.Data.FullName);
				Assert.Equal("user1", user.Data.LoginName);
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void changes_password() {
				HandledMessages.Clear();
				_users.Handle(
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "new-password", "other-password"));
				Queue.Process();
				var updateResult = HandledMessages.OfType<UserManagementMessage.UpdateResult>().Last();
				Assert.NotNull(updateResult);
				Assert.True(updateResult.Success);
			}

			[Fact]
			public void writes_password_changed_event() {
				var writePasswordChanged = HandledPasswordChangedNotificationWrites();
				Assert.Equal(1, writePasswordChanged.Length);
			}
		}

		public class when_resetting_the_password_twice : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				var replyTo = Envelope;
				yield return
					new UserManagementMessage.ResetPassword(replyTo, SystemAccount.Principal, "user1", "new-password");
				yield return
					new UserManagementMessage.ResetPassword(replyTo, SystemAccount.Principal, "user1", "new-password");
			}

			[Fact]
			public void replies_success() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(2, updateResults.Count);
				Assert.True(updateResults[0].Success);
				Assert.True(updateResults[1].Success);
			}

			[Fact]
			public void configures_password_changed_notification_system_stream_only_once() {
				var writePasswordChanged = HandledPasswordChangedNotificationMetaStreamWrites();
				Assert.Equal(1, writePasswordChanged.Length);
				var passwordChangedEvent = writePasswordChanged[0].Events.Single();
				HelperExtensions.AssertJson(new {___maxAge = 3600}, passwordChangedEvent.Data.ParseJson<JObject>());
			}
		}

		public class when_ordinary_user_attempts_to_reset_its_own_password : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.ResetPassword(Envelope, _ordinaryUser, "user1", "new-password");
			}

			[Fact]
			public void replies_unauthorized() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.False(updateResults[0].Success);
				Assert.Equal(UserManagementMessage.Error.Unauthorized, updateResults[0].Error);
			}

			[Fact]
			public void password_is_not_changed() {
				HandledMessages.Clear();
				_users.Handle(
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "Johny123!", "other-password"));
				Queue.Process();
				var updateResult = HandledMessages.OfType<UserManagementMessage.UpdateResult>().Last();
				Assert.NotNull(updateResult);
				Assert.True(updateResult.Success);
			}
		}

		public class when_changing_a_password_with_correct_current_password : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "Johny123!", "new-password");
			}

			[Fact]
			public void replies_success() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
			}

			[Fact]
			public void reply_has_the_correct_login_name() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void does_not_update_details() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal("John Doe", user.Data.FullName);
				Assert.Equal("user1", user.Data.LoginName);
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void changes_password() {
				HandledMessages.Clear();
				_users.Handle(
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "new-password", "other-password"));
				Queue.Process();
				var updateResult = HandledMessages.OfType<UserManagementMessage.UpdateResult>().Last();
				Assert.NotNull(updateResult);
				Assert.True(updateResult.Success);
			}

			[Fact]
			public void writes_password_changed_event() {
				var writePasswordChanged = HandledPasswordChangedNotificationWrites();
				Assert.Equal(1, writePasswordChanged.Length);
			}

			[Fact]
			public void configures_password_changed_notification_system_stream() {
				var writePasswordChanged = HandledPasswordChangedNotificationMetaStreamWrites();
				Assert.Equal(1, writePasswordChanged.Length);
				var passwordChangedEvent = writePasswordChanged[0].Events.Single();
				HelperExtensions.AssertJson(new {___maxAge = 3600}, passwordChangedEvent.Data.ParseJson<JObject>());
			}
		}

		public class when_changing_a_password_with_incorrect_current_password : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "incorrect", "new-password");
			}

			[Fact]
			public void replies_unauthorized() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.False(updateResults[0].Success);
				Assert.Equal(UserManagementMessage.Error.Unauthorized, updateResults[0].Error);
			}

			[Fact]
			public void reply_has_the_correct_login_name() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void does_not_update_details() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.Equal("John Doe", user.Data.FullName);
				Assert.Equal("user1", user.Data.LoginName);
				Assert.False(user.Data.Disabled);
			}

			[Fact]
			public void does_not_change_the_password() {
				HandledMessages.Clear();
				_users.Handle(
					new UserManagementMessage.ChangePassword(
						Envelope, SystemAccount.Principal, "user1", "Johny123!", "other-password"));
				Queue.Process();
				var updateResult = HandledMessages.OfType<UserManagementMessage.UpdateResult>().Last();
				Assert.NotNull(updateResult);
				Assert.True(updateResult.Success);
			}
		}

		public class when_deleting_an_existing_user_account : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				yield return
					new UserManagementMessage.Create(
						Envelope, SystemAccount.Principal, "user1", "John Doe", new[] {"admin", "other"}, "Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new UserManagementMessage.Delete(Envelope, SystemAccount.Principal, "user1");
			}

			[Fact]
			public void replies_success() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.True(updateResults[0].Success);
			}

			[Fact]
			public void reply_has_the_correct_login_name() {
				var updateResults = HandledMessages.OfType<UserManagementMessage.UpdateResult>().ToList();
				Assert.Equal(1, updateResults.Count);
				Assert.Equal("user1", updateResults[0].LoginName);
			}

			[Fact]
			public void deletes_the_user_account() {
				_users.Handle(new UserManagementMessage.Get(Envelope, SystemAccount.Principal, "user1"));
				Queue.Process();
				var user = HandledMessages.OfType<UserManagementMessage.UserDetailsResult>().SingleOrDefault();
				Assert.NotNull(user);
				Assert.False(user.Success);
				Assert.Equal(UserManagementMessage.Error.NotFound, user.Error);
				Assert.Null(user.Data);
			}

			[Fact]
			public void writes_password_changed_event() {
				var writePasswordChanged = HandledPasswordChangedNotificationWrites();
				Assert.Equal(1, writePasswordChanged.Length);
			}
		}

		public class when_getting_all_users : TestFixtureWithUserManagementService {
			protected override IEnumerable<WhenStep> GivenCommands() {
				var replyTo = Envelope;
				yield return
					new UserManagementMessage.Create(
						replyTo, SystemAccount.Principal, "user1", "John Doe 1", new[] {"admin1", "other"},
						"Johny123!");
				yield return
					new UserManagementMessage.Create(
						replyTo, SystemAccount.Principal, "user2", "John Doe 2", new[] {"admin2", "other"},
						"Johny123!");
				yield return
					new UserManagementMessage.Create(
						replyTo, SystemAccount.Principal, "user3", "Another Doe 1", new[] {"admin3", "other"},
						"Johny123!");
				yield return
					new UserManagementMessage.Create(
						replyTo, SystemAccount.Principal, "another_user", "Another Doe 2", new[] {"admin4", "other"},
						"Johny123!");
			}

			protected override IEnumerable<WhenStep> When() {
				yield return new UserManagementMessage.GetAll(Envelope, SystemAccount.Principal);
			}

			[Fact]
			public void returns_all_user_accounts() {
				var users = HandledMessages.OfType<UserManagementMessage.AllUserDetailsResult>().Single().Data;

				Assert.Equal(4, users.Length);
			}

			[Fact]
			public void returns_in_the_login_name_order() {
				var users = HandledMessages.OfType<UserManagementMessage.AllUserDetailsResult>().Single().Data;

				Assert.True(
					new[] {"another_user", "user1", "user2", "user3"}.SequenceEqual(users.Select(v => v.LoginName)));
			}

			[Fact]
			public void returns_full_names() {
				var users = HandledMessages.OfType<UserManagementMessage.AllUserDetailsResult>().Single().Data;

				Assert.True(users.Any(v => v.LoginName == "user2" && v.FullName == "John Doe 2"));
			}
		}
	}
}
