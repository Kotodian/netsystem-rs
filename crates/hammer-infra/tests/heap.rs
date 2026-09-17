//! The allocation side of the VPP-style element space: offsets are stable,
//! contiguous run starts, `len` is the element-space length the caller
//! publishes a column width from, and there is no release path.

use hammer_infra::heap::Heap;

#[test]
fn alloc_returns_consecutive_run_offsets_and_len_tracks_the_element_space() {
    let mut heap = Heap::new();
    assert!(heap.is_empty());
    assert_eq!(heap.len(), 0);

    assert_eq!(heap.alloc(3, 10u32), 0, "the first run starts at offset 0");
    assert_eq!(heap.len(), 3);
    assert!(!heap.is_empty());

    assert_eq!(heap.alloc(2, 20u32), 3, "the next run starts after it");
    assert_eq!(heap.len(), 5);

    assert_eq!(heap.alloc(1, 30u32), 5);
    assert_eq!(heap.len(), 6);
}

#[test]
fn get_reads_the_element_at_an_offset_and_none_outside_the_space() {
    let mut heap = Heap::new();
    let offset = heap.alloc(2, "column");
    assert_eq!(heap.get(offset), Some(&"column"));
    assert_eq!(heap.get(offset + 1), Some(&"column"));
    assert_eq!(heap.get(offset + 2), None);
    assert_eq!(heap.get(u32::MAX), None);
}

#[test]
#[should_panic(expected = "a heap allocation has at least one element")]
fn alloc_of_zero_elements_is_a_caller_bug() {
    let mut heap = Heap::<u32>::new();
    heap.alloc(0, 1);
}
