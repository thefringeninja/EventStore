using System;
using System.IO;
using EventStore.Core.TransactionLog.FileNamingStrategy;
using Xunit;

namespace EventStore.Core.Tests.TransactionLog {
	public class versioned_pattern_filenaming_strategy : SpecificationWithDirectory {
		[Fact]
		public void when_constructed_with_null_path_should_throws_argumentnullexception() {
			Assert.Throws<ArgumentNullException>(() => new VersionedPatternFileNamingStrategy(null, "prefix"));
		}

		[Fact]
		public void when_constructed_with_null_prefix_should_throws_argumentnullexception() {
			Assert.Throws<ArgumentNullException>(() => new VersionedPatternFileNamingStrategy("path", null));
		}

		[Fact]
		public void when_getting_file_for_positive_index_and_no_version_appends_index_to_name_with_zero_version() {
			var strategy = new VersionedPatternFileNamingStrategy("path", "prefix-");
			Assert.Equal("path" + Path.DirectorySeparatorChar + "prefix-000001.000000",
				strategy.GetFilenameFor(1, 0));
		}

		[Fact]
		public void when_getting_file_for_nonnegative_index_and_version_appends_value_and_provided_version() {
			var strategy = new VersionedPatternFileNamingStrategy("path", "prefix-");
			Assert.Equal("path" + Path.DirectorySeparatorChar + "prefix-000001.000007",
				strategy.GetFilenameFor(1, 7));
		}

		[Fact]
		public void when_getting_file_for_negative_index_throws_argumentoutofrangeexception() {
			var strategy = new VersionedPatternFileNamingStrategy("Path", "prefix");
			Assert.Throws<ArgumentOutOfRangeException>(() => strategy.GetFilenameFor(-1, 0));
		}

		[Fact]
		public void when_getting_file_for_negative_version_throws_argumentoutofrangeexception() {
			var strategy = new VersionedPatternFileNamingStrategy("Path", "prefix");
			Assert.Throws<ArgumentOutOfRangeException>(() => strategy.GetFilenameFor(0, -1));
		}

		[Fact]
		public void returns_all_existing_versions_of_the_same_chunk_in_descending_order_of_versions() {
			File.Create(GetFilePathFor("foo")).Close();
			File.Create(GetFilePathFor("bla")).Close();

			File.Create(GetFilePathFor("chunk-000001.000000")).Close();
			File.Create(GetFilePathFor("chunk-000002.000000")).Close();
			File.Create(GetFilePathFor("chunk-000003.000000")).Close();

			File.Create(GetFilePathFor("chunk-000005.000000")).Close();
			File.Create(GetFilePathFor("chunk-000005.000007")).Close();
			File.Create(GetFilePathFor("chunk-000005.000002")).Close();
			File.Create(GetFilePathFor("chunk-000005.000005")).Close();

			var strategy = new VersionedPatternFileNamingStrategy(PathName, "chunk-");
			var versions = strategy.GetAllVersionsFor(5);
			Assert.Equal(4, versions.Length);
			Assert.Equal(GetFilePathFor("chunk-000005.000007"), versions[0]);
			Assert.Equal(GetFilePathFor("chunk-000005.000005"), versions[1]);
			Assert.Equal(GetFilePathFor("chunk-000005.000002"), versions[2]);
			Assert.Equal(GetFilePathFor("chunk-000005.000000"), versions[3]);
		}

		[Fact]
		public void returns_all_existing_not_temporary_files_with_correct_pattern() {
			File.Create(GetFilePathFor("foo")).Close();
			File.Create(GetFilePathFor("bla")).Close();
			File.Create(GetFilePathFor("chunk-000001.000000.tmp")).Close();
			File.Create(GetFilePathFor("chunk-001.000")).Close();

			File.Create(GetFilePathFor("chunk-000001.000000")).Close();
			File.Create(GetFilePathFor("chunk-000002.000000")).Close();
			File.Create(GetFilePathFor("chunk-000003.000000")).Close();

			File.Create(GetFilePathFor("chunk-000005.000000")).Close();
			File.Create(GetFilePathFor("chunk-000005.000007")).Close();
			File.Create(GetFilePathFor("chunk-000005.000002")).Close();
			File.Create(GetFilePathFor("chunk-000005.000005")).Close();

			var strategy = new VersionedPatternFileNamingStrategy(PathName, "chunk-");
			var versions = strategy.GetAllPresentFiles();
			Array.Sort(versions, StringComparer.CurrentCultureIgnoreCase);
			Assert.Equal(7, versions.Length);
			Assert.Equal(GetFilePathFor("chunk-000001.000000"), versions[0]);
			Assert.Equal(GetFilePathFor("chunk-000002.000000"), versions[1]);
			Assert.Equal(GetFilePathFor("chunk-000003.000000"), versions[2]);
			Assert.Equal(GetFilePathFor("chunk-000005.000000"), versions[3]);
			Assert.Equal(GetFilePathFor("chunk-000005.000002"), versions[4]);
			Assert.Equal(GetFilePathFor("chunk-000005.000005"), versions[5]);
			Assert.Equal(GetFilePathFor("chunk-000005.000007"), versions[6]);
		}

		[Fact]
		public void returns_all_temp_files_in_directory() {
			File.Create(GetFilePathFor("bla")).Close();
			File.Create(GetFilePathFor("bla.tmp")).Close();
			File.Create(GetFilePathFor("bla.temp")).Close();

			File.Create(GetFilePathFor("chunk-000005.000007.tmp")).Close();

			File.Create(GetFilePathFor("foo.tmp")).Close();

			var strategy = new VersionedPatternFileNamingStrategy(PathName, "chunk-");
			var tempFiles = strategy.GetAllTempFiles();

			Assert.Equal(3, tempFiles.Length);
			Assert.Contains(GetFilePathFor("bla.tmp"), tempFiles);
			Assert.Contains(GetFilePathFor("chunk-000005.000007.tmp"), tempFiles);
			Assert.Contains(GetFilePathFor("foo.tmp"), tempFiles);
		}

		[Fact]
		public void does_not_return_temp_file_that_is_named_as_chunk() {
			File.Create(GetFilePathFor("chunk-000000.000000.tmp")).Close();

			var strategy = new VersionedPatternFileNamingStrategy(PathName, "chunk-");
			var tempFiles = strategy.GetAllVersionsFor(0);

			Assert.Equal(0, tempFiles.Length);
		}

		[Fact]
		public void returns_temp_filenames_detectable_by_getalltempfiles_method() {
			var strategy = new VersionedPatternFileNamingStrategy(PathName, "chunk-");
			Assert.Equal(0, strategy.GetAllTempFiles().Length);

			var tmp1 = strategy.GetTempFilename();
			var tmp2 = strategy.GetTempFilename();
			File.Create(tmp1).Close();
			File.Create(tmp2).Close();
			var tmp = new[] {tmp1, tmp2};
			Array.Sort(tmp);

			var tempFiles = strategy.GetAllTempFiles();
			Array.Sort(tempFiles);
			Assert.Equal(2, tempFiles.Length);
			Assert.Equal(tmp[0], tempFiles[0]);
			Assert.Equal(tmp[1], tempFiles[1]);
		}
	}
}
