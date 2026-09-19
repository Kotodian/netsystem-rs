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
fn released_runs_are_reused_and_adjacent_runs_are_coalesced() {
    let mut heap = Heap::new();
    let first = heap.alloc(2, 10u32);
    let second = heap.alloc(3, 20u32);
    let third = heap.alloc(2, 30u32);

    heap.remove(second);
    assert_eq!(heap.alloc(2, 40), second);
    assert_eq!(heap.get_slice(second, 2), Some([40, 40].as_slice()));

    heap.remove(second);
    heap.remove(first);
    heap.remove(third);
    assert!(heap.is_empty());
    assert_eq!(heap.alloc(7, 50), 0, "the three adjacent runs coalesce");
    assert_eq!(heap.len(), 7, "reuse does not change the high-water length");
}

#[test]
fn slices_stay_inside_one_live_allocation() {
    let mut heap = Heap::new();
    let first = heap.alloc(3, 1u32);
    let second = heap.alloc(2, 2u32);
    heap.get_slice_mut(first + 1, 2)
        .unwrap()
        .copy_from_slice(&[7, 8]);

    assert_eq!(heap.get(first), Some(&1));
    assert_eq!(heap.get_slice(first, 3), Some([1, 7, 8].as_slice()));
    assert_eq!(heap.get_slice(first + 2, 2), None);
    assert_eq!(heap.get_slice(second, 2), Some([2, 2].as_slice()));

    heap.remove(first);
    assert_eq!(heap.get(first), None);
    assert_eq!(heap.get_slice(first, 1), None);
    assert_eq!(heap.get_slice(second, 2), Some([2, 2].as_slice()));
}

#[test]
#[should_panic(expected = "a heap allocation has at least one element")]
fn alloc_of_zero_elements_is_a_caller_bug() {
    let mut heap = Heap::<u32>::new();
    heap.alloc(0, 1);
}

#[test]
#[should_panic(expected = "a heap removal names a live allocation")]
fn repeated_removal_is_a_caller_bug() {
    let mut heap = Heap::new();
    let offset = heap.alloc(1, 1u32);
    heap.remove(offset);
    heap.remove(offset);
}
