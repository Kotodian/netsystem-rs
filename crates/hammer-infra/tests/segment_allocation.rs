use hammer_infra::segment::{Segment, SegmentAllocation, SegmentAllocationError};
use std::alloc::Layout;

fn exhaust_allocations(segment: &Segment, layout: Layout) -> Vec<SegmentAllocation> {
    let mut allocations = Vec::new();
    while let Ok(allocation) = segment.allocate(layout) {
        allocations.push(allocation);
    }
    allocations
}

#[test]
fn dropping_segment_allocation_returns_capacity_after_exhaustion() {
    let page = hammer_infra::page_size().expect("checked page-size query");
    let segment = Segment::local(2 * page);
    let layout = Layout::from_size_align(64, 64).expect("valid block layout");

    let mut allocations = exhaust_allocations(&segment, layout);
    assert!(
        !allocations.is_empty(),
        "a two-page segment must hold at least one block"
    );
    assert!(
        segment.allocate(layout).is_err(),
        "the allocator must be exhausted"
    );

    drop(allocations.pop().expect("at least one live allocation"));
    let reclaimed = segment
        .allocate(layout)
        .expect("dropping the allocation must return its capacity");
    drop(reclaimed);
}

#[test]
fn into_raw_offset_keeps_block_live_until_checked_reconstruction_drops_it() {
    let page = hammer_infra::page_size().expect("checked page-size query");
    let segment = Segment::local(2 * page);
    let layout = Layout::from_size_align(64, 64).expect("valid block layout");

    let mut allocations = exhaust_allocations(&segment, layout);
    let raw = allocations
        .pop()
        .expect("at least one live allocation")
        .into_raw_offset();

    assert!(
        segment.allocate(layout).is_err(),
        "the raw block must stay allocated after into_raw_offset"
    );

    let rebuilt = unsafe { SegmentAllocation::from_raw_offset(segment.clone(), raw, layout) }
        .expect("checked reconstruction of the raw offset");
    drop(rebuilt);

    let reclaimed = segment
        .allocate(layout)
        .expect("dropping the reconstructed allocation must return capacity");
    drop(reclaimed);
}

#[test]
fn invalid_reconstruction_returns_typed_errors_without_releasing_blocks() {
    let page = hammer_infra::page_size().expect("checked page-size query");
    let segment = Segment::local(2 * page);
    let layout = Layout::from_size_align(64, 64).expect("valid block layout");

    let mut allocations = exhaust_allocations(&segment, layout);
    assert!(
        segment.allocate(layout).is_err(),
        "the allocator must be exhausted"
    );

    let out_of_bounds = unsafe {
        SegmentAllocation::from_raw_offset(segment.clone(), segment.size() as u64, layout)
    }
    .err()
    .expect("an offset at the end of the mapping must be rejected");
    assert_eq!(out_of_bounds, SegmentAllocationError::OutOfBounds);

    let overflow = unsafe { SegmentAllocation::from_raw_offset(segment.clone(), u64::MAX, layout) }
        .err()
        .expect("an offset that overflows the mapping must be rejected");
    assert_eq!(overflow, SegmentAllocationError::OutOfBounds);

    let misaligned = unsafe { SegmentAllocation::from_raw_offset(segment.clone(), 1, layout) }
        .err()
        .expect("a non-aligned offset must be rejected");
    assert_eq!(misaligned, SegmentAllocationError::Misaligned);

    let empty_layout = Layout::from_size_align(0, 1).expect("valid empty layout");
    let empty = unsafe { SegmentAllocation::from_raw_offset(segment.clone(), 0, empty_layout) }
        .err()
        .expect("a zero-size layout must be rejected");
    assert_eq!(empty, SegmentAllocationError::EmptyLayout);

    assert!(
        segment.allocate(layout).is_err(),
        "rejected reconstructions must not release any allocation"
    );

    drop(allocations.pop().expect("at least one live allocation"));
    let reclaimed = segment
        .allocate(layout)
        .expect("dropping a live allocation must return capacity");
    drop(reclaimed);
}

#[test]
fn borrowed_writable_bytes_cover_only_the_allocation() {
    let page = hammer_infra::page_size().expect("checked page-size query");
    let segment = Segment::local(2 * page);
    let layout = Layout::from_size_align(16, 8).expect("valid block layout");

    let mut allocation = segment.allocate(layout).expect("block allocation");
    assert_eq!(allocation.len(), 16);
    assert_eq!(allocation.layout(), layout);
    assert!(
        allocation.offset() as usize + allocation.len() <= segment.size(),
        "the allocation must lie inside the mapping"
    );

    let literal = *b"hammer-infra!";
    let mut expected = [0u8; 16];
    for (index, byte) in expected.iter_mut().enumerate() {
        *byte = literal[index % literal.len()];
    }
    for (index, byte) in allocation.bytes_mut().iter_mut().enumerate() {
        byte.write(expected[index]);
    }
    let initialized: &[u8] = unsafe { allocation.bytes_mut().assume_init_ref() };
    assert_eq!(initialized, &expected);
    drop(allocation);
}
