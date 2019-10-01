using System;
using System.Linq;
using Xunit;

namespace EventStore.Core.Tests.Index.AutoMergeLevelTests {
	public class when_tables_available_for_manual_merge : when_max_auto_merge_level_is_set {
		[Fact]
		public void should_merge_pending_tables_at_max_auto_merge_level() {
			AddTables(100);
			Assert.Equal(25, _result.MergedMap.InOrder().Count());
			var (level, table) = _result.MergedMap.GetTableForManualMerge();
			Assert.True(level > 2);
			_result = _result.MergedMap.AddPTable(table, _result.MergedMap.PrepareCheckpoint,
				_result.MergedMap.CommitCheckpoint, UpgradeHash, ExistsAt,
				RecordExistsAt, _fileNameProvider, _ptableVersion,
				level: level,
				skipIndexVerify: _skipIndexVerify);
			Assert.Equal(1, _result.MergedMap.InOrder().Count());
		}
	}
}
