using System;
using EventStore.Core.Services.Transport.Tcp;
using Xunit;

namespace EventStore.Core.Tests.Services.Transport.Tcp {
	public class core_tcp_package {
		[Fact]
		public void should_throw_argument_null_exception_when_created_as_authorized_but_login_not_provided() {
			Assert.Throws<ArgumentNullException>(() =>
				new TcpPackage(TcpCommand.BadRequest, TcpFlags.Authenticated, Guid.NewGuid(), null, "pa$$",
					new byte[] {1, 2, 3}));
		}

		[Fact]
		public void should_throw_argument_null_exception_when_created_as_authorized_but_password_not_provided() {
			Assert.Throws<ArgumentNullException>(() =>
				new TcpPackage(TcpCommand.BadRequest, TcpFlags.Authenticated, Guid.NewGuid(), "login", null,
					new byte[] {1, 2, 3}));
		}

		[Fact]
		public void should_throw_argument_exception_when_created_as_not_authorized_but_login_is_provided() {
			Assert.Throws<ArgumentException>(() =>
				new TcpPackage(TcpCommand.BadRequest, TcpFlags.None, Guid.NewGuid(), "login", null,
					new byte[] {1, 2, 3}));
		}

		[Fact]
		public void should_throw_argument_exception_when_created_as_not_authorized_but_password_is_provided() {
			Assert.Throws<ArgumentException>(() =>
				new TcpPackage(TcpCommand.BadRequest, TcpFlags.None, Guid.NewGuid(), null, "pa$$",
					new byte[] {1, 2, 3}));
		}

		[Fact]
		public void not_authorized_with_data_should_serialize_and_deserialize_correctly() {
			var corrId = Guid.NewGuid();
			var refPkg = new TcpPackage(TcpCommand.BadRequest, TcpFlags.None, corrId, null, null, new byte[] {1, 2, 3});
			var bytes = refPkg.AsArraySegment();

			var pkg = TcpPackage.FromArraySegment(bytes);
			Assert.Equal(TcpCommand.BadRequest, pkg.Command);
			Assert.Equal(TcpFlags.None, pkg.Flags);
			Assert.Equal(corrId, pkg.CorrelationId);
			Assert.Null(pkg.Login);
			Assert.Null(pkg.Password);

			Assert.Equal(3, pkg.Data.Count);
			Assert.Equal(1, pkg.Data.Array[pkg.Data.Offset + 0]);
			Assert.Equal(2, pkg.Data.Array[pkg.Data.Offset + 1]);
			Assert.Equal(3, pkg.Data.Array[pkg.Data.Offset + 2]);
		}

		[Fact]
		public void not_authorized_with_empty_data_should_serialize_and_deserialize_correctly() {
			var corrId = Guid.NewGuid();
			var refPkg = new TcpPackage(TcpCommand.BadRequest, TcpFlags.None, corrId, null, null, new byte[0]);
			var bytes = refPkg.AsArraySegment();

			var pkg = TcpPackage.FromArraySegment(bytes);
			Assert.Equal(TcpCommand.BadRequest, pkg.Command);
			Assert.Equal(TcpFlags.None, pkg.Flags);
			Assert.Equal(corrId, pkg.CorrelationId);
			Assert.Null(pkg.Login);
			Assert.Null(pkg.Password);

			Assert.Equal(0, pkg.Data.Count);
		}

		[Fact]
		public void authorized_with_data_should_serialize_and_deserialize_correctly() {
			var corrId = Guid.NewGuid();
			var refPkg = new TcpPackage(TcpCommand.BadRequest, TcpFlags.Authenticated, corrId, "login", "pa$$",
				new byte[] {1, 2, 3});
			var bytes = refPkg.AsArraySegment();

			var pkg = TcpPackage.FromArraySegment(bytes);
			Assert.Equal(TcpCommand.BadRequest, pkg.Command);
			Assert.Equal(TcpFlags.Authenticated, pkg.Flags);
			Assert.Equal(corrId, pkg.CorrelationId);
			Assert.Equal("login", pkg.Login);
			Assert.Equal("pa$$", pkg.Password);

			Assert.Equal(3, pkg.Data.Count);
			Assert.Equal(1, pkg.Data.Array[pkg.Data.Offset + 0]);
			Assert.Equal(2, pkg.Data.Array[pkg.Data.Offset + 1]);
			Assert.Equal(3, pkg.Data.Array[pkg.Data.Offset + 2]);
		}

		[Fact]
		public void authorized_with_empty_data_should_serialize_and_deserialize_correctly() {
			var corrId = Guid.NewGuid();
			var refPkg = new TcpPackage(TcpCommand.BadRequest, TcpFlags.Authenticated, corrId, "login", "pa$$",
				new byte[0]);
			var bytes = refPkg.AsArraySegment();

			var pkg = TcpPackage.FromArraySegment(bytes);
			Assert.Equal(TcpCommand.BadRequest, pkg.Command);
			Assert.Equal(TcpFlags.Authenticated, pkg.Flags);
			Assert.Equal(corrId, pkg.CorrelationId);
			Assert.Equal("login", pkg.Login);
			Assert.Equal("pa$$", pkg.Password);

			Assert.Equal(0, pkg.Data.Count);
		}

		[Fact]
		public void should_throw_argument_exception_on_serialization_when_login_too_long() {
			var pkg = new TcpPackage(TcpCommand.BadRequest, TcpFlags.Authenticated, Guid.NewGuid(),
				new string('*', 256), "pa$$", new byte[] {1, 2, 3});
			Assert.Throws<ArgumentException>(() => pkg.AsByteArray());
		}

		[Fact]
		public void should_throw_argument_exception_on_serialization_when_password_too_long() {
			var pkg = new TcpPackage(TcpCommand.BadRequest, TcpFlags.Authenticated, Guid.NewGuid(), "login",
				new string('*', 256), new byte[] {1, 2, 3});
			Assert.Throws<ArgumentException>(() => pkg.AsByteArray());
		}
	}
}
