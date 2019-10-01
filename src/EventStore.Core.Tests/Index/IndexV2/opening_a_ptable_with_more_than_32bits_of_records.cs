using System;
using System.Security.Cryptography;
using System.Diagnostics;
using System.IO;
using Xunit;
using EventStore.Common.Utils;
using EventStore.Common.Options;

namespace EventStore.Core.Tests.Index.IndexV2 {
	public class
		opening_a_ptable_with_more_than_32bits_of_records : IndexV1.opening_a_ptable_with_more_than_32bits_of_records {
		public opening_a_ptable_with_more_than_32bits_of_records() : base() {
		}
	}
}
