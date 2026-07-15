use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_infra::boxed::Box;
use hammer_infra::map::FlatHashTable;
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
    assert_eq!(mem::size_of::<Vec<u64>>(), 3 * mem::size_of::<usize>());

    ALLOCATIONS.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.with(|count| count.set(true));

    let empty_values = Vec::<u64>::with_capacity(0);
    let mut values = Vec::new();
    for value in 0..256 {
        values.push(value);
    }
    let clone = values.clone();
    std::hint::black_box((&values, &clone));

    let repeated = hammer_infra::vec![7_u64; 3];
    let listed = hammer_infra::vec![1_u64, 2, 3];
    std::hint::black_box((&repeated, &listed));

    let slice: Box<[u64]> = Box::from_elem(256, 7_u64);
    let slice_clone = slice.clone();
    let empty_slice: Box<[u64]> = Box::from_elem(0, 0);
    let generated: Box<[u64]> = Box::from_fn(64, |index| index as u64);
    std::hint::black_box((&slice, &slice_clone, &empty_slice, &generated));

    let boxed = clone.into_boxed_slice();
    std::hint::black_box(&boxed);

    let mut consumed = values.into_iter();
    assert_eq!(consumed.next(), Some(0));

    drop(consumed);
    drop(boxed);
    drop(generated);
    drop(empty_slice);
    drop(slice_clone);
    drop(slice);
    drop(listed);
    drop(repeated);
    drop(empty_values);

    COUNT_ALLOCATIONS.with(|count| count.set(false));
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
}

#[test]
fn default_hash_table_lifecycle_uses_main_heap_for_string_index() {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.with(|count| count.set(true));

    let mut table = FlatHashTable::new();
    for (index, name) in [
        "tun",
        "ip",
        "tcp",
        "udp",
        "device",
        "interface",
        "transport",
        "session",
    ]
    .into_iter()
    .enumerate()
    {
        table.insert(name, index);
    }
    assert_eq!(table.get(&"tcp"), Some(&2));
    assert_eq!(table.remove(&"session"), Some(7));
    let clone = table.clone();
    assert_eq!(clone.get(&"transport"), Some(&6));
    std::hint::black_box((&table, &clone));
    drop(clone);
    drop(table);

    COUNT_ALLOCATIONS.with(|count| count.set(false));
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
}
