use std::sync::Arc;
use std::thread;

use hammer_runtime::sync::{
    RwLock, SpinLock, compiler_barrier, memory_barrier, release_fence, store_barrier,
};

#[test]
fn spin_lock_serializes_writers() {
    let value = Arc::new(SpinLock::new(0_u32));
    let mut writers = Vec::new();

    for _ in 0..4 {
        let value = Arc::clone(&value);
        writers.push(thread::spawn(move || {
            for _ in 0..10_000 {
                *value.lock() += 1;
            }
        }));
    }

    for writer in writers {
        writer.join().expect("spin-lock writer");
    }

    assert_eq!(*value.lock(), 40_000);
}

#[test]
fn spin_lock_try_lock_reports_contention() {
    let value = SpinLock::new(7_u32);
    let guard = value.lock();

    assert!(value.try_lock().is_none());
    drop(guard);
    assert_eq!(*value.try_lock().expect("released spin lock"), 7);
}

#[test]
fn rw_lock_allows_readers_and_serializes_writers() {
    let value = Arc::new(RwLock::new(0_u32));
    let mut writers = Vec::new();

    for _ in 0..4 {
        let value = Arc::clone(&value);
        writers.push(thread::spawn(move || {
            for _ in 0..10_000 {
                *value.write() += 1;
            }
        }));
    }

    for writer in writers {
        writer.join().expect("rw-lock writer");
    }

    let reader = value.read();
    let another_reader = value.read();
    assert_eq!((*reader, *another_reader), (40_000, 40_000));
}

#[test]
fn vpp_memory_barriers_are_callable() {
    compiler_barrier();
    release_fence();
    store_barrier();
    memory_barrier();
}
