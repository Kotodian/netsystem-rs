use std::sync::Arc;

use hammer_adapter::buffer::{
    BUFFER_THREAD_CACHE_BATCH, BUFFER_THREAD_CACHE_HIGH_WATER, BufferPool, BufferPoolArena,
    DEFAULT_PRE_DATA_SIZE, buffer_data_offset,
};
use hammer_infra::heap::Heap;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn one_contiguous_region_header_and_data_inline() {
    let pool = BufferPool::with_capacity(2048, 64);
    let index = pool.alloc_index().expect("alloc");

    pool.append(index, &[0xAB; 100]).expect("append");

    let buffer = pool.get(index).expect("buffer");
    let header_ptr = std::ptr::from_ref(&*buffer) as usize;
    let current_ptr = buffer.current_ptr() as usize;

    assert_eq!(current_ptr - header_ptr, buffer_data_offset());
    assert_eq!(buffer.current(), &[0xAB; 100]);
}

#[test]
fn index_to_pointer_is_slot_times_stride() {
    let pool = BufferPool::with_capacity(2048, 8);
    let stride = pool.slot_stride();
    let base = pool.base_ptr() as usize;
    let slot = 5u32;
    let got = pool.buffer_raw_ptr(slot) as usize;

    assert_eq!(got, base + slot as usize * stride);
}

#[test]
fn slot_zero_is_reserved_like_vpp_invalid_buffer_index() {
    let pool = BufferPool::with_capacity(2048, 8);
    let first = pool.alloc_index().expect("first alloc");
    assert_ne!(first.slot(), 0, "slot 0 must not be handed out");
    assert_eq!(pool.buffer_raw_ptr(0) as usize, pool.base_ptr() as usize);
}

#[test]
fn negative_current_data_points_into_pre_data_headroom() {
    let pool = BufferPool::with_capacity(2048, 32);
    let index = pool.alloc_index().expect("alloc");

    pool.append(index, &[0u8; 32]).expect("append payload");
    pool.prepend(index, &[0x42; 32]).expect("prepend header");

    let buffer = pool.get(index).expect("buffer");
    assert_eq!(buffer.current_data_offset(), -32);
    assert_eq!(&buffer.current()[..32], &[0x42; 32]);
    assert_eq!(
        pool.data_raw_ptr(index.slot()) as usize - buffer.current_ptr() as usize,
        32
    );
    assert_eq!(DEFAULT_PRE_DATA_SIZE, 128);
}

#[test]
fn per_thread_cache_constants_match_local_vpp() {
    assert_eq!(BUFFER_THREAD_CACHE_BATCH, 32);
    assert_eq!(BUFFER_THREAD_CACHE_HIGH_WATER, 512);
}

#[test]
fn arena_allocates_one_heap_region_and_returns_it_on_drop() {
    let region = SvmRegion::with_size(1 << 20);
    let heap = Arc::new(Heap::svm(region.clone(), 0));
    let region_start = region.base() as usize;
    let region_end = region_start + region.size();

    {
        let arena = BufferPoolArena::with_capacity_in(2048, 64, heap.clone());
        let pool = BufferPool::with_arena(arena);
        let base = pool.base_ptr() as usize;
        assert_eq!(base % 64, 0);
        assert!(base >= region_start);
        assert!(base < region_end);

        let first = pool.alloc_index().expect("alloc");
        assert_eq!(
            pool.buffer_raw_ptr(first.slot()) as usize,
            base + first.slot() as usize * pool.slot_stride()
        );
    }

    {
        let arena = BufferPoolArena::with_capacity_in(2048, 64, heap);
        let pool = BufferPool::with_arena(arena);
        let base = pool.base_ptr() as usize;
        assert_eq!(base % 64, 0);
        assert!(base >= region_start);
        assert!(base < region_end);
    }
}
