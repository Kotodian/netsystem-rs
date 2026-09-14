//! Public configuration checks for the owner-initialized shared queue.

#![cfg(target_os = "linux")]

use hammer_infra::svm::queue::{
    SvmQueue, SvmQueueConditionalWait, SvmQueueConfig, SvmQueueError, SvmQueueOperation,
};
use std::mem::size_of;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::ptr::NonNull;
use std::time::Duration;

#[test]
fn queue_layout_accounts_for_inline_data() {
    let config = SvmQueueConfig {
        nels: 4,
        elsize: 8,
        consumer_pid: -1,
    };
    let size = SvmQueue::size_to_alloc(&config).expect("queue layout");
    assert_eq!(size, size_of::<SvmQueue>() + 4 * 8);
    #[cfg(target_arch = "x86_64")]
    assert_eq!(size_of::<SvmQueue>(), 120);
}

#[test]
fn queue_uses_shared_inline_slots_and_stack_typed_receive() {
    let config = SvmQueueConfig {
        nels: 4,
        elsize: 8,
        consumer_pid: 7,
    };
    let bytes = SvmQueue::size_to_alloc(&config).expect("queue layout");
    let allocation = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(allocation, libc::MAP_FAILED);
    let base = NonNull::new(allocation.cast::<u8>()).expect("mapped queue address");
    assert_eq!(base.as_ptr().addr() % 64, 0);
    let address = unsafe { SvmQueue::init(base, &config) }.expect("initialize queue");
    let queue = unsafe { address.as_ref() };
    assert_eq!(queue.consumer_pid(), 7);
    assert_eq!(
        unsafe { queue.ring_head_slot_unlocked() }.as_ptr().addr(),
        base.as_ptr().addr() + size_of::<SvmQueue>()
    );
    assert!(matches!(
        unsafe { SvmQueue::attach(base, bytes - 1) },
        Err(SvmQueueError::InvalidHeader)
    ));
    let attached = unsafe { SvmQueue::attach(base, bytes) }.expect("attach same mapping");
    assert_eq!(attached, address);
    assert!(queue.can_send());
    assert!(matches!(queue.try_sub_element::<u64>(), Ok(None)));
    queue
        .add_element(&42_u64, SvmQueueConditionalWait::Wait)
        .expect("enqueue address-sized element");
    assert_eq!(
        queue
            .sub_element::<u64>(SvmQueueConditionalWait::Nowait)
            .expect("stack receive"),
        42
    );
    if size_of::<usize>() == queue.element_size() {
        let payload = [0x5A_u8; 32];
        let address = payload.as_ptr() as usize;
        queue
            .add(&address.to_ne_bytes(), SvmQueueConditionalWait::Wait)
            .expect("enqueue only the payload address");
        let mut received_address = [0_u8; size_of::<usize>()];
        queue
            .sub(&mut received_address, SvmQueueConditionalWait::Wait)
            .expect("dequeue only the payload address");
        assert_eq!(usize::from_ne_bytes(received_address), address);
        assert_eq!(payload, [0x5A_u8; 32]);
    }
    std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(5));
            queue
                .add_element(&81_u64, SvmQueueConditionalWait::Wait)
                .expect("wake waiting consumer");
        });
        assert_eq!(
            queue
                .sub_element::<u64>(SvmQueueConditionalWait::Wait)
                .expect("waiting consumer receives element"),
            81
        );
    });
    for index in 0..4_u64 {
        queue
            .add_element(&index, SvmQueueConditionalWait::Nowait)
            .expect("fill queue");
    }
    assert!(!queue.can_send());
    assert!(matches!(
        queue.add_element(&99_u64, SvmQueueConditionalWait::Nowait),
        Err(SvmQueueError::QueueFull)
    ));
    for index in 0..4_u64 {
        assert_eq!(
            queue
                .sub_element::<u64>(SvmQueueConditionalWait::Nowait)
                .expect("drain full queue"),
            index
        );
    }
    assert!(matches!(
        queue.sub_element::<u32>(SvmQueueConditionalWait::Nowait),
        Err(SvmQueueError::ElementSizeMismatch {
            requested: 4,
            stored: 8
        })
    ));
    assert!(matches!(
        queue.sub_element::<u64>(SvmQueueConditionalWait::TimedWait(Duration::from_millis(1))),
        Err(SvmQueueError::Timeout)
    ));
    let mut descriptors = [0; 2];
    assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
    let read_descriptor = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
    let write_descriptor = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
    unsafe { queue.set_producer_event_fd(read_descriptor.as_fd()) };
    let error = queue
        .add_element(&77_u64, SvmQueueConditionalWait::Wait)
        .expect_err("writing to the read end cannot notify");
    assert!(matches!(error,
        SvmQueueError::EventSignalAfterCommit {
            operation: SvmQueueOperation::Add,
            source,
        } if source.raw_os_error() == Some(libc::EBADF)));
    assert_eq!(queue.len().expect("committed element remains queued"), 1);
    assert_eq!(
        queue
            .sub_element::<u64>(SvmQueueConditionalWait::Nowait)
            .expect("consume element after notification failure"),
        77
    );
    unsafe { queue.set_consumer_event_fd(read_descriptor.as_fd()) };
    assert!(matches!(
        queue.add_element(&0_u64, SvmQueueConditionalWait::Wait),
        Err(SvmQueueError::EventSignalAfterCommit {
            operation: SvmQueueOperation::Add,
            ..
        })
    ));
    for index in 1..4_u64 {
        queue
            .add_element(&index, SvmQueueConditionalWait::Wait)
            .expect("queue fills after the committed notification error");
    }
    assert!(matches!(
        queue.sub_element::<u64>(SvmQueueConditionalWait::Nowait),
        Err(SvmQueueError::EventSignalAfterCommit {
            operation: SvmQueueOperation::Sub,
            source,
        }) if source.raw_os_error() == Some(libc::EBADF)
    ));
    assert_eq!(queue.len().expect("committed dequeue reduced occupancy"), 3);
    for index in 1..4_u64 {
        assert_eq!(
            queue
                .sub_element::<u64>(SvmQueueConditionalWait::Nowait)
                .expect("read the remaining queued elements"),
            index
        );
    }
    let mut guard = queue.lock().expect("ring lock");
    let first_slot = guard.ring_head_slot();
    guard.advance_ring_head();
    let slot_base = base.as_ptr().addr() + size_of::<SvmQueue>();
    let last_slot = slot_base + (queue.capacity() - 1) * queue.element_size();
    let next_slot = if first_slot.as_ptr().addr() == last_slot {
        slot_base
    } else {
        first_slot.as_ptr().addr() + queue.element_size()
    };
    assert_eq!(guard.ring_head_slot().as_ptr().addr(), next_slot);
    drop(guard);
    assert_eq!(queue.len().expect("ring does not affect occupancy"), 0);
    unsafe { queue.reset_mutex_for_restart() };
    assert!(queue.try_lock().is_ok());
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let guard = queue.lock().expect("worker owns robust mutex");
            std::mem::forget(guard);
        });
    });
    assert!(matches!(queue.lock(), Err(SvmQueueError::OwnerDied)));
    assert!(queue.lock().is_ok());
    unsafe { queue.cleanup() };
    assert_eq!(unsafe { libc::munmap(allocation, bytes) }, 0);
    drop(write_descriptor);
    drop(read_descriptor);
}

#[test]
fn queue_layout_rejects_zero_capacity_and_element_size() {
    let zero_capacity = SvmQueueConfig {
        nels: 0,
        elsize: 8,
        consumer_pid: -1,
    };
    assert!(matches!(
        SvmQueue::size_to_alloc(&zero_capacity),
        Err(SvmQueueError::ZeroCapacity)
    ));

    let zero_element_size = SvmQueueConfig {
        nels: 1,
        elsize: 0,
        consumer_pid: -1,
    };
    assert!(matches!(
        SvmQueue::size_to_alloc(&zero_element_size),
        Err(SvmQueueError::ZeroElementSize)
    ));
}

#[test]
fn queue_restart_resets_mutex_after_abandoned_owner() {
    let config = SvmQueueConfig {
        nels: 1,
        elsize: size_of::<usize>() as u32,
        consumer_pid: -1,
    };
    let bytes = SvmQueue::size_to_alloc(&config).expect("queue layout");
    let allocation = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(allocation, libc::MAP_FAILED);
    let base = NonNull::new(allocation.cast::<u8>()).expect("mapped queue address");
    let address = unsafe { SvmQueue::init(base, &config) }.expect("initialize queue");
    let queue = unsafe { address.as_ref() };
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let guard = queue.lock().expect("worker acquired queue mutex");
            std::mem::forget(guard);
        });
    });
    let started = std::time::Instant::now();
    unsafe { queue.reset_mutex_for_restart() };
    assert!(started.elapsed() >= Duration::from_millis(90));
    assert!(queue.try_lock().is_ok());
    unsafe { queue.cleanup() };
    assert_eq!(unsafe { libc::munmap(allocation, bytes) }, 0);
}
