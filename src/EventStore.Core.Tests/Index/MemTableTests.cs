using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Utils;
using EventStore.Core.Index;
using Xunit;

namespace EventStore.Core.Tests.Index {
	public class HashListMemTableTests : MemTableTestsFixture {
		public HashListMemTableTests()
			: base(() => new HashListMemTable(PTableVersions.IndexV2, maxSize: 20)) {
		}
	}

	public abstract class MemTableTestsFixture {
		private readonly Func<IMemTable> _memTableFactory;

		protected IMemTable MemTable;

		protected MemTableTestsFixture(Func<IMemTable> memTableFactory) {
			_memTableFactory = memTableFactory;
			Ensure.NotNull(memTableFactory, "memTableFactory");
			MemTable = _memTableFactory();
		}

		[Fact]
		public void throw_argumentoutofrangeexception_on_range_query_when_provided_with_negative_start_version() {
			Assert.Throws<ArgumentOutOfRangeException>(() => MemTable.GetRange(0x0000, -1, int.MaxValue).ToArray());
		}

		[Fact]
		public void throw_argumentoutofrangeexception_on_range_query_when_provided_with_negative_end_version() {
			Assert.Throws<ArgumentOutOfRangeException>(() => MemTable.GetRange(0x0000, 0, -1).ToArray());
		}

		[Fact]
		public void throw_argumentoutofrangeexception_on_get_one_entry_query_when_provided_with_negative_version() {
			long pos;
			Assert.Throws<ArgumentOutOfRangeException>(() => MemTable.TryGetOneValue(0x0000, -1, out pos));
		}

		[Fact]
		public void throw_argumentoutofrangeexception_on_adding_entry_with_negative_version() {
			Assert.Throws<ArgumentOutOfRangeException>(() => MemTable.Add(0x0000, -1, 0));
		}

		[Fact]
		public void throw_argumentoutofrangeexception_on_adding_entry_with_negative_position() {
			Assert.Throws<ArgumentOutOfRangeException>(() => MemTable.Add(0x0000, 0, -1));
		}

		[Fact]
		public void empty_memtable_has_count_of_zero() {
			Assert.Equal(0, MemTable.Count);
		}

		[Fact]
		public void adding_an_item_increments_count() {
			MemTable.Add(0x11, 0x01, 0xffff);
			Assert.Equal(1, MemTable.Count);
		}

		[Fact]
		public void non_existent_item_is_not_found() {
			MemTable.Add(0x11, 0x01, 0xffff);
			long position;
			Assert.False(MemTable.TryGetOneValue(0x11, 0x02, out position));
		}


		[Fact]
		public void existing_item_is_found() {
			MemTable.Add(0x11, 0x01, 0xffff);
			long position;
			Assert.True(MemTable.TryGetOneValue(0x11, 0x01, out position));
		}

		[Fact]
		public void items_come_out_sorted() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			var list = new List<IndexEntry>(MemTable.IterateAllInOrder());
			Assert.Equal(3, list.Count);
			Assert.Equal(0x12UL, list[0].Stream);
			Assert.Equal(0x01, list[0].Version);
			Assert.Equal(0xfff3, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x02, list[1].Version);
			Assert.Equal(0xfff2, list[1].Position);
			Assert.Equal(0x11UL, list[2].Stream);
			Assert.Equal(0x01, list[2].Version);
			Assert.Equal(0xfff1, list[2].Position);
		}

		[Fact]
		public void items_come_out_sorted_with_duplicates_in_descending_order() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x12, 0x01, 0xfff4);
			var list = new List<IndexEntry>(MemTable.IterateAllInOrder());
			Assert.Equal(3, list.Count);
			Assert.Equal(0x12UL, list[0].Stream);
			Assert.Equal(0x01, list[0].Version);
			Assert.Equal(0xfff4, list[0].Position);
			Assert.Equal(0x12UL, list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff3, list[1].Position);
			Assert.Equal(0x11UL, list[2].Stream);
			Assert.Equal(0x01, list[2].Version);
			Assert.Equal(0xfff1, list[2].Position);
		}

		[Fact]
		public void can_do_range_query_of_existing_items() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x01, 0x02));
			Assert.Equal(2, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x02, list[0].Version);
			Assert.Equal(0xfff2, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff1, list[1].Position);
		}

		[Fact]
		public void can_do_range_query_of_existing_items_with_duplicates_on_edges() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x01, 0xfff5);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			MemTable.Add(0x11, 0x02, 0xfff7);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x01, 0x02));
			Assert.Equal(4, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x02, list[0].Version);
			Assert.Equal(0xfff7, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x02, list[1].Version);
			Assert.Equal(0xfff2, list[1].Position);
			Assert.Equal(0x11UL, list[2].Stream);
			Assert.Equal(0x01, list[2].Version);
			Assert.Equal(0xfff5, list[2].Position);
			Assert.Equal(0x11UL, list[3].Stream);
			Assert.Equal(0x01, list[3].Version);
			Assert.Equal(0xfff1, list[3].Position);
		}

		[Fact]
		public void range_query_of_non_existing_stream_returns_nothing() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			var list = new List<IndexEntry>(MemTable.GetRange(0x14, 0x01, 0x02));
			Assert.Equal(0, list.Count);
		}

		[Fact]
		public void range_query_of_non_existing_version_returns_nothing() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			var list = new List<IndexEntry>(MemTable.GetRange(0x01, 0x03, 0x05));
			Assert.Equal(0, list.Count);
		}


		[Fact]
		public void range_query_with_hole_returns_items_included() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			MemTable.Add(0x11, 0x04, 0xfff4);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x01, 0x04));
			Assert.Equal(3, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x04, list[0].Version);
			Assert.Equal(0xfff4, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x02, list[1].Version);
			Assert.Equal(0xfff2, list[1].Position);
			Assert.Equal(0x11UL, list[2].Stream);
			Assert.Equal(0x01, list[2].Version);
			Assert.Equal(0xfff1, list[2].Position);
		}

		[Fact]
		public void query_with_start_in_range_but_not_end_results_returns_items_included() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x01, 0x04));
			Assert.Equal(2, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x02, list[0].Version);
			Assert.Equal(0xfff2, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff1, list[1].Position);
		}

		[Fact]
		public void query_with_end_in_range_but_not_start_results_returns_items_included() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x00, 0x02));
			Assert.Equal(2, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x02, list[0].Version);
			Assert.Equal(0xfff2, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff1, list[1].Position);
		}

		[Fact]
		public void query_with_end_and_start_exclusive_results_returns_items_included() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x12, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff2);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x00, 0x04));
			Assert.Equal(2, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x02, list[0].Version);
			Assert.Equal(0xfff2, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff1, list[1].Position);
		}

		[Fact]
		public void query_with_end_inside_the_hole_in_list_returns_items_included() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x03, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x00, 0x04));
			Assert.Equal(2, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x03, list[0].Version);
			Assert.Equal(0xfff3, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x01, list[1].Version);
			Assert.Equal(0xfff1, list[1].Position);
		}

		[Fact]
		public void query_with_end_inside_the_hole_in_list_returns_items_included_with_duplicates() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x03, 0xfff3);
			MemTable.Add(0x11, 0x03, 0xfff2);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x00, 0x04));
			Assert.Equal(3, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x03, list[0].Version);
			Assert.Equal(0xfff3, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x03, list[1].Version);
			Assert.Equal(0xfff2, list[1].Position);
			Assert.Equal(0x11UL, list[2].Stream);
			Assert.Equal(0x01, list[2].Version);
			Assert.Equal(0xfff1, list[2].Position);
		}

		[Fact]
		public void query_with_start_inside_the_hole_in_list_returns_items_included() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x03, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x02, 0x06));
			Assert.Equal(2, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x05, list[0].Version);
			Assert.Equal(0xfff5, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x03, list[1].Version);
			Assert.Equal(0xfff3, list[1].Position);
		}

		[Fact]
		public void query_with_start_inside_the_hole_in_list_returns_duplicated_items_included() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x03, 0xfff3);
			MemTable.Add(0x11, 0x03, 0xfff2);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x02, 0x06));
			Assert.Equal(3, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x05, list[0].Version);
			Assert.Equal(0xfff5, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x03, list[1].Version);
			Assert.Equal(0xfff3, list[1].Position);
			Assert.Equal(0x11UL, list[2].Stream);
			Assert.Equal(0x03, list[2].Version);
			Assert.Equal(0xfff2, list[2].Position);
		}

		[Fact]
		public void query_with_start_and_end_inside_the_hole_in_list_returns_items_included() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x03, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x02, 0x04));
			Assert.Equal(1, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x03, list[0].Version);
			Assert.Equal(0xfff3, list[0].Position);
		}

		[Fact]
		public void query_with_start_and_end_inside_the_hole_in_list_returns_items_included_with_duplicates() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x01, 0xfff2);
			MemTable.Add(0x11, 0x03, 0xfff3);
			MemTable.Add(0x11, 0x03, 0xfff4);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x02, 0x04));
			Assert.Equal(2, list.Count);
			Assert.Equal(0x11UL, list[0].Stream);
			Assert.Equal(0x03, list[0].Version);
			Assert.Equal(0xfff4, list[0].Position);
			Assert.Equal(0x11UL, list[1].Stream);
			Assert.Equal(0x03, list[1].Version);
			Assert.Equal(0xfff3, list[1].Position);
		}

		[Fact]
		public void query_with_start_and_end_less_than_all_items_returns_nothing() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x03, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x00, 0x00));
			Assert.Equal(0, list.Count);
		}

		[Fact]
		public void query_with_start_and_end_greater_than_all_items_returns_nothing() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x03, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var list = new List<IndexEntry>(MemTable.GetRange(0x11, 0x06, 0x06));
			Assert.Equal(0, list.Count);
		}

		[Fact]
		public void try_get_one_value_returns_value_with_highest_position() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			long position;
			Assert.True(MemTable.TryGetOneValue(0x11, 0x01, out position));
			Assert.Equal(0xfff3, position);
		}

		[Fact]
		public void get_range_of_same_version_returns_both_values_in_descending_order_when_duplicated() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			var entries = MemTable.GetRange(0x11, 0x01, 0x01).ToArray();
			Assert.Equal(2, entries.Length);
			Assert.Equal(0x11UL, entries[0].Stream);
			Assert.Equal(0x01, entries[0].Version);
			Assert.Equal(0xfff3, entries[0].Position);
			Assert.Equal(0x11UL, entries[1].Stream);
			Assert.Equal(0x01, entries[1].Version);
			Assert.Equal(0xfff1, entries[1].Position);
		}

		[Fact]
		public void try_get_one_value_returns_the_value_with_largest_position_when_triduplicated() {
			MemTable.Add(0x01, 0x05, 0xfff9);
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			MemTable.Add(0x11, 0x01, 0xfff2);

			long position;
			Assert.True(MemTable.TryGetOneValue(0x11, 0x01, out position));
			Assert.Equal(0xfff3, position);
		}

		[Fact]
		public void get_range_of_same_version_returns_both_values_in_descending_order_when_triduplicated() {
			MemTable.Add(0x01, 0x05, 0xfff9);
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x05, 0xfff5);
			MemTable.Add(0x11, 0x01, 0xfff2);

			var entries = MemTable.GetRange(0x11, 0x01, 0x01).ToArray();
			Assert.Equal(3, entries.Length);
			Assert.Equal(0x11UL, entries[0].Stream);
			Assert.Equal(0x01, entries[0].Version);
			Assert.Equal(0xfff3, entries[0].Position);

			Assert.Equal(0x11UL, entries[1].Stream);
			Assert.Equal(0x01, entries[1].Version);
			Assert.Equal(0xfff2, entries[1].Position);

			Assert.Equal(0x11UL, entries[2].Stream);
			Assert.Equal(0x01, entries[2].Version);
			Assert.Equal(0xfff1, entries[2].Position);
		}

		[Fact]
		public void try_get_latest_entry_finds_nothing_on_empty_memtable() {
			IndexEntry entry;
			Assert.False(MemTable.TryGetLatestEntry(0x12, out entry));
		}

		[Fact]
		public void try_get_latest_entry_finds_nothing_on_empty_stream() {
			MemTable.Add(0x11, 0x01, 0xffff);
			IndexEntry entry;
			Assert.False(MemTable.TryGetLatestEntry(0x12, out entry));
		}

		[Fact]
		public void single_item_is_latest() {
			MemTable.Add(0x11, 0x01, 0xffff);
			IndexEntry entry;
			Assert.True(MemTable.TryGetLatestEntry(0x11, out entry));
			Assert.Equal(0x11UL, entry.Stream);
			Assert.Equal(0x01, entry.Version);
			Assert.Equal(0xffff, entry.Position);
		}

		[Fact]
		public void try_get_latest_entry_returns_correct_entry() {
			MemTable.Add(0x11, 0x01, 0xffff);
			MemTable.Add(0x11, 0x02, 0xfff2);
			IndexEntry entry;
			Assert.True(MemTable.TryGetLatestEntry(0x11, out entry));
			Assert.Equal(0x11UL, entry.Stream);
			Assert.Equal(0x02, entry.Version);
			Assert.Equal(0xfff2, entry.Position);
		}

		[Fact]
		public void try_get_latest_entry_when_duplicated_entries_returns_the_one_with_largest_position() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x02, 0xfff2);
			MemTable.Add(0x11, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff4);
			IndexEntry entry;
			Assert.True(MemTable.TryGetLatestEntry(0x11, out entry));
			Assert.Equal(0x11UL, entry.Stream);
			Assert.Equal(0x02, entry.Version);
			Assert.Equal(0xfff4, entry.Position);
		}

		[Fact]
		public void try_get_latest_entry_returns_the_entry_with_the_largest_position_when_triduplicated() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x01, 0xfff5);
			IndexEntry entry;
			Assert.True(MemTable.TryGetLatestEntry(0x11, out entry));
			Assert.Equal(0x11UL, entry.Stream);
			Assert.Equal(0x01, entry.Version);
			Assert.Equal(0xfff5, entry.Position);
		}

		[Fact]
		public void try_get_oldest_entry_finds_nothing_on_empty_memtable() {
			IndexEntry entry;
			Assert.False(MemTable.TryGetOldestEntry(0x12, out entry));
		}

		[Fact]
		public void try_get_oldest_entry_finds_nothing_on_empty_stream() {
			MemTable.Add(0x11, 0x01, 0xffff);
			IndexEntry entry;
			Assert.False(MemTable.TryGetOldestEntry(0x12, out entry));
		}

		[Fact]
		public void single_item_is_oldest() {
			MemTable.Add(0x11, 0x01, 0xffff);
			IndexEntry entry;
			Assert.True(MemTable.TryGetOldestEntry(0x11, out entry));
			Assert.Equal(0x11UL, entry.Stream);
			Assert.Equal(0x01, entry.Version);
			Assert.Equal(0xffff, entry.Position);
		}

		[Fact]
		public void try_get_oldest_entry_returns_correct_entry() {
			MemTable.Add(0x11, 0x01, 0xffff);
			MemTable.Add(0x11, 0x02, 0xfff2);
			IndexEntry entry;
			Assert.True(MemTable.TryGetOldestEntry(0x11, out entry));
			Assert.Equal(0x11UL, entry.Stream);
			Assert.Equal(0x01, entry.Version);
			Assert.Equal(0xffff, entry.Position);
		}

		[Fact]
		public void try_get_oldest_entry_when_duplicated_entries_returns_the_one_with_smallest_position() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x02, 0xfff2);
			MemTable.Add(0x11, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x02, 0xfff4);
			IndexEntry entry;
			Assert.True(MemTable.TryGetOldestEntry(0x11, out entry));
			Assert.Equal(0x11UL, entry.Stream);
			Assert.Equal(0x01, entry.Version);
			Assert.Equal(0xfff1, entry.Position);
		}

		[Fact]
		public void try_get_oldest_entry_returns_the_entry_with_the_smallest_position_when_triduplicated() {
			MemTable.Add(0x11, 0x01, 0xfff1);
			MemTable.Add(0x11, 0x01, 0xfff3);
			MemTable.Add(0x11, 0x01, 0xfff5);
			IndexEntry entry;
			Assert.True(MemTable.TryGetOldestEntry(0x11, out entry));
			Assert.Equal(0x11UL, entry.Stream);
			Assert.Equal(0x01, entry.Version);
			Assert.Equal(0xfff1, entry.Position);
		}


		[Fact]
		public void the_smallest_items_with_hash_collisions_can_be_found() {
			MemTable.Add(0, 0, 0x0001);
			MemTable.Add(0, 0, 0x0002);
			MemTable.Add(1, 0, 0x0003);
			MemTable.Add(1, 0, 0x0004);
			MemTable.Add(1, 0, 0x0005);

			long position;
			Assert.True(MemTable.TryGetOneValue(0, 0, out position));
			Assert.Equal(0x0002, position);
		}

		[Fact]
		public void the_smallest_items_with_hash_collisions_are_returned_in_descending_order() {
			MemTable.Add(0, 0, 0x0001);
			MemTable.Add(0, 0, 0x0002);
			MemTable.Add(1, 0, 0x0003);
			MemTable.Add(1, 0, 0x0004);
			MemTable.Add(1, 0, 0x0005);

			var entries = MemTable.GetRange(0, 0, 0).ToArray();
			Assert.Equal(2, entries.Length);
			Assert.Equal(0UL, entries[0].Stream);
			Assert.Equal(0, entries[0].Version);
			Assert.Equal(0x0002, entries[0].Position);
			Assert.Equal(0UL, entries[1].Stream);
			Assert.Equal(0, entries[1].Version);
			Assert.Equal(0x0001, entries[1].Position);
		}

		[Fact]
		public void try_get_latest_entry_for_smallest_hash_with_collisions_returns_correct_index_entry() {
			MemTable.Add(0, 0, 0x0001);
			MemTable.Add(0, 0, 0x0002);
			MemTable.Add(1, 0, 0x0003);
			MemTable.Add(1, 0, 0x0004);
			MemTable.Add(1, 0, 0x0005);

			IndexEntry entry;
			Assert.True(MemTable.TryGetLatestEntry(0, out entry));
			Assert.Equal(0UL, entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0x0002, entry.Position);
		}

		[Fact]
		public void try_get_oldest_entry_for_smallest_hash_with_collisions_returns_correct_index_entry() {
			MemTable.Add(0, 0, 0x0001);
			MemTable.Add(0, 0, 0x0002);
			MemTable.Add(1, 0, 0x0003);
			MemTable.Add(1, 0, 0x0004);
			MemTable.Add(1, 0, 0x0005);

			IndexEntry entry;
			Assert.True(MemTable.TryGetOldestEntry(0, out entry));
			Assert.Equal(0UL, entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0x0001, entry.Position);
		}

		[Fact]
		public void the_largest_items_with_hash_collisions_can_be_found() {
			MemTable.Add(0, 0, 0x0001);
			MemTable.Add(0, 0, 0x0002);
			MemTable.Add(1, 0, 0x0003);
			MemTable.Add(1, 0, 0x0004);
			MemTable.Add(1, 0, 0x0005);

			long position;
			Assert.True(MemTable.TryGetOneValue(1, 0, out position));
			Assert.Equal(0x0005, position);
		}

		[Fact]
		public void the_largest_items_with_hash_collisions_are_returned_in_descending_order() {
			MemTable.Add(0, 0, 0x0001);
			MemTable.Add(0, 0, 0x0002);
			MemTable.Add(1, 0, 0x0003);
			MemTable.Add(1, 0, 0x0004);
			MemTable.Add(1, 0, 0x0005);

			var entries = MemTable.GetRange(1, 0, 0).ToArray();
			Assert.Equal(3, entries.Length);
			Assert.Equal(1UL, entries[0].Stream);
			Assert.Equal(0, entries[0].Version);
			Assert.Equal(0x0005, entries[0].Position);
			Assert.Equal(1UL, entries[1].Stream);
			Assert.Equal(0, entries[1].Version);
			Assert.Equal(0x0004, entries[1].Position);
			Assert.Equal(1UL, entries[2].Stream);
			Assert.Equal(0, entries[2].Version);
			Assert.Equal(0x0003, entries[2].Position);
		}

		[Fact]
		public void try_get_latest_entry_for_largest_hash_collision_returns_correct_index_entry() {
			MemTable.Add(0, 0, 0x0001);
			MemTable.Add(0, 0, 0x0002);
			MemTable.Add(1, 0, 0x0003);
			MemTable.Add(1, 0, 0x0004);
			MemTable.Add(1, 0, 0x0005);

			IndexEntry entry;
			Assert.True(MemTable.TryGetLatestEntry(1, out entry));
			Assert.Equal(1UL, entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0x0005, entry.Position);
		}

		[Fact]
		public void try_get_oldest_entry_for_largest_hash_with_collisions_returns_correct_index_entry() {
			MemTable.Add(0, 0, 0x0001);
			MemTable.Add(0, 0, 0x0002);
			MemTable.Add(1, 0, 0x0003);
			MemTable.Add(1, 0, 0x0004);
			MemTable.Add(1, 0, 0x0005);

			IndexEntry entry;
			Assert.True(MemTable.TryGetOldestEntry(1, out entry));
			Assert.Equal(1UL, entry.Stream);
			Assert.Equal(0, entry.Version);
			Assert.Equal(0x0003, entry.Position);
		}
	}
}
