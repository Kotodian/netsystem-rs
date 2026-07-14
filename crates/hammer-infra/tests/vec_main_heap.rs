use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_infra::boxed::Slice;
use hammer_infra::heap::Heap;
use hammer_infra::vec::Vec;

struct CountingAllocator;

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNT_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[test]
fn default_heap_lifecycles_avoid_the_process_global_allocator() {
    let explicit_main = Arc::new(Heap::main());
    let explicit_main_ref = Arc::downgrade(&explicit_main);

    ALLOCATIONS.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.with(|count| count.set(true));

    let main = Heap::main();
    let main_clone = main.clone();

    let mut values = Vec::new();
    for value in 0..256 {
        values.push(value);
    }
    let clone = values.clone();
    std::hint::black_box((&values, &clone));

    let slice = Slice::from_elem(256, 7_u64);
    let slice_clone = slice.clone();
    std::hint::black_box((&slice, &slice_clone));

    let boxed = clone.into_boxed_slice();
    std::hint::black_box(&boxed);

    let mut consumed = values.into_iter();
    assert_eq!(consumed.next(), Some(0));

    let explicit_main_values = Vec::<u64>::with_capacity_in(16, explicit_main);
    let retained_explicit_main_refs = explicit_main_ref.strong_count();

    drop(explicit_main_values);
    drop(consumed);
    drop(boxed);
    drop(slice_clone);
    drop(slice);
    drop(main_clone);
    drop(main);

    COUNT_ALLOCATIONS.with(|count| count.set(false));
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(retained_explicit_main_refs, 0);
}
