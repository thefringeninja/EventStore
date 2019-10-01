using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace EventStore.Core.Tests.Index.AutoMergeLevelTests {
	public class when_auto_merge_level_is_zero : when_max_auto_merge_level_is_set {
		public when_auto_merge_level_is_zero() : base(0) {
		}

		[Fact]
		public void manual_merge_should_merge_all_tables() {
			AddTables(11);
			var (level, table) = _result.MergedMap.GetTableForManualMerge();
			_result = _result.MergedMap.AddPTable(table, -1, -1, UpgradeHash, ExistsAt, RecordExistsAt,
				_fileNameProvider, _ptableVersion, level);
			Assert.Equal(1, _result.MergedMap.InOrder().Count());
		}
	}
}
