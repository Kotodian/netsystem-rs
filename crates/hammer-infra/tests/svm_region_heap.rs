//! Behavior tests for the SVM region heap (ADR-0011 section 12.4).
//!
//! The heap allocates from a caller-owned byte arena, so every test builds one
//! explicitly. A violation terminates the process, therefore the negative
//! cases run in a child process and assert both the abort and the structured
//! fact it reported.

use std::alloc::Layout;
use std::process::Command;

use hammer_infra::svm::region_heap::SvmRegionHeap;

/// Fixed space in front of the heap inside the arena, so that a heap offset of
/// zero stays distinguishable from the zero value that ends a free-list link.
const HEAP_START: u64 = 64;
const HEAP_SIZE: u64 = 64 * 1024;
/// Header plus user prefix: the first usable user offset is `HEAP_START + 24`.
const BLOCK_OVERHEAD: u64 = 24;
/// Smallest block the heap hands out: header, user prefix, and free-list links.
const MIN_BLOCK: u64 = 40;
const CHILD_CASE: &str = "HAMMER_SVM_REGION_HEAP_VIOLATION_CASE";

/// Block bytes the heap charges for `size`: the request is rounded up to the
/// block alignment and carries the block header and the user prefix.
fn block_bytes(size: u64) -> u64 {
    ((size + BLOCK_OVERHEAD + 7) & !7).max(MIN_BLOCK)
}

fn layout(size: usize, alignment: usize) -> Layout {
    Layout::from_size_align(size, alignment).expect("test layout")
}

fn arena() -> Vec<u8> {
    vec![0u8; (HEAP_START + HEAP_SIZE) as usize]
}

fn heap() -> (Vec<u8>, SvmRegionHeap) {
    let mut arena = arena();
    let mut heap = SvmRegionHeap::new();
    heap.initialize(&mut arena, HEAP_START, HEAP_START + HEAP_SIZE);
    assert_eq!(heap.used_bytes(), 0, "a fresh heap has no live block");
    assert_eq!(
        heap.free_bytes(),
        HEAP_SIZE,
        "a fresh heap is one free block"
    );
    assert_eq!(heap.peak_used_bytes(), 0);
    (arena, heap)
}

#[test]
fn allocations_are_aligned_disjoint_and_zeroed() {
    let (mut arena, mut heap) = heap();
    let requests = [
        (1usize, 8usize),
        (16, 8),
        (24, 16),
        (100, 64),
        (4096, 8),
        (7, 256),
    ];
    let mut ranges: Vec<(u64, u64)> = Vec::new();

    for (size, alignment) in requests {
        let request = layout(size, alignment);
        let offset = heap.allocate(&mut arena, request);
        assert_eq!(
            offset % alignment as u64,
            0,
            "alignment {alignment} for size {size}"
        );
        assert!(offset >= HEAP_START + BLOCK_OVERHEAD);
        assert!(offset + size as u64 <= HEAP_START + HEAP_SIZE);
        assert!(
            ranges
                .iter()
                .all(|(start, end)| offset + size as u64 <= *start || offset >= *end),
            "allocation {offset} overlaps {ranges:?}"
        );
        ranges.push((offset, offset + size as u64));

        assert_eq!(heap.bytes_at(&arena, offset, size as u64), vec![0u8; size]);
        heap.bytes_at_mut(&mut arena, offset, size as u64)
            .fill(0xa5);
        assert_eq!(
            heap.bytes_at(&arena, offset, size as u64),
            vec![0xa5u8; size]
        );
        assert!(heap.holds(&arena, offset, request));
    }

    assert_eq!(heap.free_bytes() + heap.used_bytes(), HEAP_SIZE);
    assert_eq!(heap.peak_used_bytes(), heap.used_bytes());
    assert!(heap.peak_used_bytes() >= 4096);
}

#[test]
fn block_accounting_includes_the_block_header() {
    let (mut arena, mut heap) = heap();
    let request = layout(100, 8);
    let offset = heap.allocate(&mut arena, request);

    assert_eq!(offset, HEAP_START + BLOCK_OVERHEAD);
    assert_eq!(heap.used_bytes(), block_bytes(100));
    assert_eq!(heap.free_bytes(), HEAP_SIZE - block_bytes(100));
    assert_eq!(heap.peak_used_bytes(), heap.used_bytes());
}

#[test]
fn alignment_above_the_block_minimum_is_honored() {
    let (mut arena, mut heap) = heap();
    let request = layout(64, 64);
    let offset = heap.allocate(&mut arena, request);

    assert_eq!(offset % 64, 0);
    assert!(heap.holds(&arena, offset, request));
    assert!(
        !heap.holds(&arena, offset, layout(64, 4096)),
        "the same block cannot satisfy a stronger alignment"
    );
}

#[test]
fn free_then_allocate_reuses_the_same_block() {
    let (mut arena, mut heap) = heap();
    let request = layout(64, 8);
    let first = heap.allocate(&mut arena, request);
    heap.bytes_at_mut(&mut arena, first, 64).fill(0x11);
    heap.deallocate(&mut arena, first, request);

    let second = heap.allocate(&mut arena, request);
    assert_eq!(first, second);
    assert_eq!(heap.bytes_at(&arena, second, 64), vec![0u8; 64]);
}

#[test]
fn adjacent_blocks_coalesce_back_into_one_block() {
    let (mut arena, mut heap) = heap();
    let request = layout(1024, 8);
    let first = heap.allocate(&mut arena, request);
    let middle = heap.allocate(&mut arena, request);
    let last = heap.allocate(&mut arena, request);
    assert!(first < middle && middle < last);

    heap.deallocate(&mut arena, first, request);
    heap.deallocate(&mut arena, last, request);
    assert_eq!(heap.free_bytes() + heap.used_bytes(), HEAP_SIZE);

    heap.deallocate(&mut arena, middle, request);
    assert_eq!(heap.used_bytes(), 0);
    assert_eq!(heap.free_bytes(), HEAP_SIZE);

    let whole = heap.allocate(&mut arena, layout((HEAP_SIZE - BLOCK_OVERHEAD) as usize, 8));
    assert_eq!(whole, HEAP_START + BLOCK_OVERHEAD);
    assert_eq!(heap.free_bytes(), 0);
    assert_eq!(heap.used_bytes(), HEAP_SIZE);
}

#[test]
fn freeing_merges_with_the_previous_free_block() {
    let (mut arena, mut heap) = heap();
    let request = layout(1024, 8);
    let first = heap.allocate(&mut arena, request);
    let second = heap.allocate(&mut arena, request);
    let third = heap.allocate(&mut arena, request);

    heap.deallocate(&mut arena, second, request);
    heap.deallocate(&mut arena, first, request);
    assert_eq!(
        heap.used_bytes(),
        1024 + BLOCK_OVERHEAD,
        "only the third block is live"
    );

    let merged = heap.allocate(&mut arena, layout(2000, 8));
    assert_eq!(merged, first, "the two freed blocks merged back into one");
    assert!(heap.holds(&arena, merged, layout(2000, 8)));
    assert!(heap.holds(&arena, third, request));
}

#[test]
fn many_blocks_fill_and_release_the_whole_heap() {
    let (mut arena, mut heap) = heap();
    let request = layout(100, 8);
    let block = block_bytes(100);
    let mut offsets = Vec::new();
    for _ in 0..500 {
        offsets.push(heap.allocate(&mut arena, request));
    }
    let mut sorted = offsets.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), offsets.len(), "distinct blocks");
    assert_eq!(heap.used_bytes(), 500 * block);
    assert!(heap.used_bytes() < HEAP_SIZE);

    for offset in offsets {
        heap.deallocate(&mut arena, offset, request);
    }
    assert_eq!(heap.used_bytes(), 0);
    assert_eq!(heap.free_bytes(), HEAP_SIZE);
    assert_eq!(heap.peak_used_bytes(), 500 * block);

    let whole = heap.allocate(&mut arena, layout((HEAP_SIZE - BLOCK_OVERHEAD) as usize, 8));
    assert_eq!(whole, HEAP_START + BLOCK_OVERHEAD);
}

#[test]
fn reallocation_grows_by_copying_and_shrinks_in_place() {
    let (mut arena, mut heap) = heap();
    let request = layout(32, 8);
    let offset = heap.allocate(&mut arena, request);
    heap.bytes_at_mut(&mut arena, offset, 32).fill(0x5a);

    let grown = heap.reallocate(&mut arena, offset, request, 4096);
    assert_eq!(heap.bytes_at(&arena, grown, 32), vec![0x5au8; 32]);
    assert_eq!(heap.free_bytes() + heap.used_bytes(), HEAP_SIZE);
    assert!(heap.holds(&arena, grown, layout(4096, 8)));

    let shrunk = heap.reallocate(&mut arena, grown, layout(4096, 8), 64);
    assert_eq!(shrunk, grown, "a shrink keeps the offset");
    let bytes = heap.bytes_at(&arena, shrunk, 64);
    assert_eq!(bytes[..32], vec![0x5au8; 32], "the copied bytes survive");
    assert_eq!(bytes[32..], vec![0u8; 32], "the grown tail was zeroed");
    assert!(heap.holds(&arena, shrunk, layout(64, 8)));
    assert_eq!(heap.free_bytes() + heap.used_bytes(), HEAP_SIZE);

    let reused = heap.allocate(&mut arena, layout(64, 8));
    assert_ne!(reused, shrunk, "the released tail is allocatable again");
}

#[test]
fn shrink_that_cannot_split_keeps_the_block() {
    let (mut arena, mut heap) = heap();
    let offset = heap.allocate(&mut arena, layout(4096, 8));
    let used = heap.used_bytes();

    let same = heap.reallocate(&mut arena, offset, layout(4096, 8), 4080);
    assert_eq!(same, offset);
    assert_eq!(
        heap.used_bytes(),
        used,
        "a tail below the minimum block size stays allocated"
    );
    assert!(heap.holds(&arena, same, layout(4080, 8)));
}

#[test]
fn zero_sized_request_round_trips() {
    let (mut arena, mut heap) = heap();
    let request = layout(0, 8);
    let offset = heap.allocate(&mut arena, request);
    assert_eq!(offset % 8, 0);
    assert_eq!(heap.bytes_at(&arena, offset, 0).len(), 0);
    assert!(heap.holds(&arena, offset, request));
    assert_eq!(heap.used_bytes(), MIN_BLOCK);

    heap.deallocate(&mut arena, offset, request);
    assert_eq!(heap.used_bytes(), 0);
    assert_eq!(heap.free_bytes(), HEAP_SIZE);
}

#[test]
fn holds_rejects_interior_unaligned_and_oversized_ranges() {
    let (mut arena, mut heap) = heap();
    let request = layout(64, 16);
    let offset = heap.allocate(&mut arena, request);

    assert!(
        !heap.holds(&arena, offset + 16, layout(64, 16)),
        "interior offset"
    );
    assert!(
        !heap.holds(&arena, offset + 16, layout(8, 8)),
        "interior offset with a fitting range"
    );
    assert!(
        !heap.holds(&arena, offset, layout(4096, 16)),
        "range beyond the block"
    );
    assert!(
        !heap.holds(&arena, HEAP_START, request),
        "offset before the first user area"
    );
    assert!(
        !heap.holds(&arena, offset, layout(64, 4096)),
        "stronger alignment than the block"
    );
    assert!(heap.holds(&arena, offset, request));

    heap.deallocate(&mut arena, offset, request);
    assert!(!heap.holds(&arena, offset, request), "released block");
}

#[test]
fn uninitialized_heap_queries_are_empty_and_reject_offsets() {
    let mut arena = arena();
    let mut heap = SvmRegionHeap::new();

    assert_eq!(heap.free_bytes(), 0);
    assert_eq!(heap.used_bytes(), 0);
    assert_eq!(heap.peak_used_bytes(), 0);
    assert!(!heap.holds(&arena, HEAP_START + BLOCK_OVERHEAD, layout(8, 8)));

    heap.initialize(&mut arena, HEAP_START, HEAP_START + HEAP_SIZE);
    assert_eq!(heap.free_bytes(), HEAP_SIZE);
    let offset = heap.allocate(&mut arena, layout(8, 8));
    assert!(heap.holds(&arena, offset, layout(8, 8)));
}

#[test]
fn mixed_allocation_sequence_keeps_the_heap_consistent() {
    let alignments = [8usize, 16, 64];

    for seed in [
        0x2545_f491_4f6c_dd1du64,
        0x9e37_79b9_7f4a_7c15,
        0x0bad_c0de_dead_beef,
    ] {
        let (mut arena, mut heap) = heap();
        let mut state = seed;
        let mut live: Vec<(u64, Layout)> = Vec::new();

        for step in 0..2000 {
            let choice = next_random(&mut state);
            if live.len() >= 64 || (choice.is_multiple_of(3) && !live.is_empty()) {
                let index = (next_random(&mut state) as usize) % live.len();
                let (offset, request) = live.swap_remove(index);
                if choice.is_multiple_of(2) {
                    let size = (next_random(&mut state) % 512) as usize + 1;
                    let offset = heap.reallocate(&mut arena, offset, request, size);
                    live.push((offset, layout(size, request.align())));
                } else {
                    heap.deallocate(&mut arena, offset, request);
                }
            } else {
                let size = (next_random(&mut state) % 512) as usize + 1;
                let alignment = alignments[(next_random(&mut state) % 3) as usize];
                let request = layout(size, alignment);
                live.push((heap.allocate(&mut arena, request), request));
            }

            assert_eq!(
                heap.free_bytes() + heap.used_bytes(),
                HEAP_SIZE,
                "accounting at step {step}"
            );
            let mut ranges: Vec<(u64, u64)> = Vec::new();
            for (offset, request) in &live {
                if !heap.holds(&arena, *offset, *request) {
                    let back = word_at(&arena, *offset - 8);
                    panic!(
                        "step {step}: lost the block at {offset} ({request:?}); prefix word {back}, header {:?}, previous {:#x}",
                        word_at(&arena, back + 8),
                        word_at(&arena, back),
                    );
                }
                assert_eq!(offset % request.align() as u64, 0, "step {step} alignment");
                ranges.push((*offset, offset + request.size() as u64));
            }
            ranges.sort_unstable();
            for pair in ranges.windows(2) {
                assert!(
                    pair[0].1 <= pair[1].0,
                    "step {step} overlapping live blocks {pair:?}"
                );
            }
        }

        let peak = heap.peak_used_bytes();
        for (offset, request) in live.drain(..) {
            heap.deallocate(&mut arena, offset, request);
        }
        assert_eq!(heap.used_bytes(), 0, "seed {seed:#x} released every block");
        assert_eq!(
            heap.free_bytes(),
            HEAP_SIZE,
            "seed {seed:#x} freed the heap"
        );
        assert!(peak > 0, "seed {seed:#x} never allocated");
        let whole = heap.allocate(&mut arena, layout((HEAP_SIZE - BLOCK_OVERHEAD) as usize, 8));
        assert_eq!(
            whole,
            HEAP_START + BLOCK_OVERHEAD,
            "seed {seed:#x} coalesced back"
        );
    }
}

fn word_at(arena: &[u8], offset: u64) -> u64 {
    u64::from_ne_bytes(
        arena[offset as usize..offset as usize + 8]
            .try_into()
            .unwrap(),
    )
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn region_heap_violations_terminate_the_process() {
    if let Ok(case) = std::env::var(CHILD_CASE) {
        trigger_violation(&case);
        return;
    }

    let cases = [
        ("double_free", "already free"),
        ("exhaustion", "exhausted"),
        ("misaligned_deallocate", "not aligned to"),
        ("unallocated_deallocate", "is not a live allocation"),
        ("foreign_bytes", "is not a live allocation"),
        ("range_beyond_allocation", "exceeds heap end"),
        ("invalid_range", "is not a valid block range"),
        ("zero_sized_reallocation", "reallocate to zero size"),
        ("oversized_reallocation", "layout overflow"),
        ("corrupted_block_header", "is corrupt"),
    ];

    for (case, reported) in cases {
        let output = Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "region_heap_violations_terminate_the_process",
                "--nocapture",
            ])
            .env(CHILD_CASE, case)
            .output()
            .expect("child process");
        assert!(
            !output.status.success(),
            "case {case} continued after the violation"
        );
        assert_eq!(
            output.status.code(),
            None,
            "case {case} exited instead of aborting"
        );
        let reported_fact = String::from_utf8_lossy(&output.stderr);
        assert!(
            reported_fact.contains(reported),
            "case {case} reported {reported_fact:?}, expected it to contain {reported:?}"
        );
    }
}

fn trigger_violation(case: &str) {
    match case {
        "invalid_range" => {
            let mut arena = arena();
            let mut heap = SvmRegionHeap::new();
            heap.initialize(&mut arena, HEAP_START, HEAP_START + 16);
        }
        "corrupted_block_header" => {
            let (mut arena, mut heap) = heap();
            arena[HEAP_START as usize + 8..HEAP_START as usize + 16]
                .copy_from_slice(&u64::MAX.to_ne_bytes());
            heap.allocate(&mut arena, layout(64, 8));
        }
        _ => {
            let (mut arena, mut heap) = heap();
            let request = layout(64, 8);
            match case {
                "double_free" => {
                    let offset = heap.allocate(&mut arena, request);
                    heap.deallocate(&mut arena, offset, request);
                    heap.deallocate(&mut arena, offset, request);
                }
                "exhaustion" => {
                    heap.allocate(&mut arena, layout(HEAP_SIZE as usize, 8));
                }
                "misaligned_deallocate" => {
                    let offset = heap.allocate(&mut arena, request);
                    heap.deallocate(&mut arena, offset + 74, request);
                }
                "unallocated_deallocate" => {
                    let offset = heap.allocate(&mut arena, request);
                    heap.deallocate(&mut arena, offset + 16, layout(8, 8));
                }
                "foreign_bytes" => {
                    heap.bytes_at(&arena, HEAP_START, 8);
                }
                "range_beyond_allocation" => {
                    let offset = heap.allocate(&mut arena, request);
                    heap.bytes_at(&arena, offset, 4096);
                }
                "zero_sized_reallocation" => {
                    let offset = heap.allocate(&mut arena, request);
                    heap.reallocate(&mut arena, offset, request, 0);
                }
                "oversized_reallocation" => {
                    let offset = heap.allocate(&mut arena, request);
                    heap.reallocate(&mut arena, offset, request, usize::MAX);
                }
                other => panic!("unknown violation case {other}"),
            }
        }
    }
}
