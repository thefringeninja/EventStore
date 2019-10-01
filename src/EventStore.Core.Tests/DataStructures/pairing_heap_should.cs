using System;
using System.Collections.Generic;
using EventStore.Core.DataStructures;
using Xunit;

namespace EventStore.Core.Tests.DataStructures {
	public class pairing_heap_should : IDisposable {
		PairingHeap<int> _heap;

		public pairing_heap_should() {
			_heap = new PairingHeap<int>();
		}

		public void Dispose() {
			_heap = null;
		}

		[Fact]
		public void throw_argumentnullexception_when_given_null_comparer() {
			Assert.Throws<ArgumentNullException>(() => new PairingHeap<int>(null as IComparer<int>));
		}

		[Fact]
		public void throw_argumentnullexception_when_given_null_compare_func() {
			Assert.Throws<ArgumentNullException>(() => new PairingHeap<int>(null as Func<int, int, bool>));
		}

		[Fact]
		public void throw_invalidoperationexception_when_trying_to_find_min_element_on_empty_queue() {
			Assert.Throws<InvalidOperationException>(() => _heap.FindMin());
		}

		[Fact]
		public void throw_invalidoperationexception_when_trying_to_delete_min_element_on_empty_queue() {
			Assert.Throws<InvalidOperationException>(() => _heap.DeleteMin());
		}

		[Fact]
		public void return_correct_min_element_and_keep_it_in_heap_on_findmin_operation() {
			_heap.Add(9);
			_heap.Add(7);
			_heap.Add(5);
			_heap.Add(3);

			Assert.Equal(3, _heap.FindMin());
			Assert.Equal(4, _heap.Count);
		}

		[Fact]
		public void return_correct_min_element_and_remove_it_from_heap_on_delete_min_operation() {
			_heap.Add(7);
			_heap.Add(5);
			_heap.Add(3);

			Assert.Equal(3, _heap.DeleteMin());
			Assert.Equal(2, _heap.Count);
		}

		[Fact]
		public void return_elements_in_sorted_order() {
			var reference = new[] {2, 5, 7, 9, 11, 27, 32};
			var returned = new List<int>();

			for (int i = reference.Length - 1; i >= 0; --i) {
				_heap.Add(reference[i]);
			}

			while (_heap.Count > 0) {
				returned.Add(_heap.DeleteMin());
			}

			Assert.Equal(returned, reference);
		}

		[Fact]
		public void keep_all_duplicates() {
			var reference = new[] {2, 5, 5, 7, 9, 9, 11, 11, 11, 27, 32};
			var returned = new List<int>();

			for (int i = reference.Length - 1; i >= 0; --i) {
				_heap.Add(reference[i]);
			}

			while (_heap.Count > 0) {
				returned.Add(_heap.DeleteMin());
			}

			Assert.Equal(returned, reference);
		}

		[Fact]
		public void handle_a_lot_of_elements_and_not_loose_any_elements() {
			var elements = new List<int>();
			var rnd = new Random(123456791);

			for (int i = 0; i < 1000; ++i) {
				var elem = rnd.Next();
				elements.Add(elem);
				_heap.Add(elem);
			}

			elements.Sort();

			var returned = new List<int>();
			while (_heap.Count > 0) {
				returned.Add(_heap.DeleteMin());
			}
		}
	}
}
