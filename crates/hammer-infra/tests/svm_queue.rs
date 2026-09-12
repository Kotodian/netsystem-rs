//! Behavior tests for the VPP-style shared-memory queue.

#![cfg(target_os = "linux")]

use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::thread;
use std::time::Duration;

use hammer_infra::svm::queue::{SvmQueue, SvmQueueConditionalWait, SvmQueueConfig, SvmQueueError};
use hammer_infra::svm::ssvm::{SSVM_PAYLOAD_OFFSET, SsvmConfig, SsvmPrivate, SsvmSegmentBackend};

const SEGMENT_BYTES: usize = 1 << 16;
const QUEUE_OFFSET: u64 = SSVM_PAYLOAD_OFFSET + 4096;

fn segment(name: &str) -> SsvmPrivate {
    SsvmPrivate::server_init_memfd(&SsvmConfig {
        backend: SsvmSegmentBackend::Memfd,
        name: name.to_string(),
        size: SEGMENT_BYTES,
        requested_va: 0,
        huge_page: false,
        attach_timeout: Duration::from_millis(100),
    })
    .expect("memfd segment")
}

fn queue(segment: &SsvmPrivate, capacity: u32) -> SvmQueue {
    unsafe {
        SvmQueue::init_at(
            segment,
            QUEUE_OFFSET,
            &SvmQueueConfig {
                nels: capacity,
                elsize: size_of::<u64>() as u32,
                consumer_pid: std::process::id() as i32,
            },
        )
    }
    .expect("queue initialization")
}

fn bytes(value: u64) -> [u8; size_of::<u64>()] {
    value.to_ne_bytes()
}

fn value(bytes: &[u8]) -> u64 {
    u64::from_ne_bytes(bytes.try_into().expect("u64 bytes"))
}

#[test]
fn queue_preserves_fifo_order_and_reports_vpp_metadata() {
    let segment = segment("hammer-svm-queue-order");
    let queue = queue(&segment, 4);

    assert_eq!(queue.capacity(), 4);
    assert_eq!(queue.element_size(), size_of::<u64>());
    assert_eq!(queue.consumer_pid(), std::process::id() as i32);

    queue.add(&bytes(10), false).expect("first add");
    queue.add(&bytes(20), false).expect("second add");
    assert_eq!(queue.len().expect("queue length"), 2);
    let mut output = [0; size_of::<u64>()];
    queue
        .sub(&mut output, SvmQueueConditionalWait::Nowait)
        .expect("first dequeue");
    assert_eq!(value(&output), 10);
    queue
        .sub(&mut output, SvmQueueConditionalWait::Nowait)
        .expect("second dequeue");
    assert_eq!(value(&output), 20);
    assert!(queue.is_empty().expect("empty queue"));

    unsafe { queue.destroy().expect("queue destroy") };
}

#[test]
fn queue_add2_is_atomic_when_two_slots_are_unavailable() {
    let segment = segment("hammer-svm-queue-add2");
    let queue = queue(&segment, 2);

    queue.add2(&bytes(1), &bytes(2), false).expect("pair add");
    assert!(queue.is_full().expect("full queue"));
    assert!(matches!(
        queue.add2(&bytes(3), &bytes(4), true),
        Err(SvmQueueError::QueueFull)
    ));
    let mut output = [0; size_of::<u64>()];
    queue
        .sub(&mut output, SvmQueueConditionalWait::Nowait)
        .expect("first dequeue");
    assert_eq!(value(&output), 1);
    queue
        .sub(&mut output, SvmQueueConditionalWait::Nowait)
        .expect("second dequeue");
    assert_eq!(value(&output), 2);
    assert_eq!(queue.sub2(&mut output).unwrap(), false);

    unsafe { queue.destroy().expect("queue destroy") };
}

#[test]
fn queue_wait_wakes_after_producer_adds_an_element() {
    let segment = segment("hammer-svm-queue-wait");
    let queue = queue(&segment, 2);
    let consumer = unsafe { SvmQueue::attach(&segment, QUEUE_OFFSET) }.expect("consumer attach");
    let producer = unsafe { SvmQueue::attach(&segment, QUEUE_OFFSET) }.expect("producer attach");

    let worker = thread::spawn(move || {
        let mut output = [0; size_of::<u64>()];
        consumer
            .sub(&mut output, SvmQueueConditionalWait::Wait)
            .map(|()| value(&output))
    });
    thread::sleep(Duration::from_millis(20));
    producer.add(&bytes(99), false).expect("producer add");
    assert_eq!(
        worker.join().expect("consumer worker").expect("dequeue"),
        99
    );

    unsafe { queue.destroy().expect("queue destroy") };
}

#[test]
fn queue_timedwait_returns_timeout_and_nowait_distinguishes_empty() {
    let segment = segment("hammer-svm-queue-timeout");
    let queue = queue(&segment, 1);
    let mut output = [0; size_of::<u64>()];

    assert!(matches!(
        queue.sub(&mut output, SvmQueueConditionalWait::Nowait),
        Err(SvmQueueError::Empty)
    ));
    assert!(matches!(
        queue.sub(
            &mut output,
            SvmQueueConditionalWait::TimedWait(Duration::from_millis(20))
        ),
        Err(SvmQueueError::Timeout)
    ));

    unsafe { queue.destroy().expect("queue destroy") };
}

#[test]
fn queue_wraps_a_non_power_of_two_ring() {
    let segment = segment("hammer-svm-queue-wrap");
    let queue = queue(&segment, 3);
    let mut output = [0; size_of::<u64>()];

    for item in [1, 2, 3] {
        queue.add(&bytes(item), true).expect("enqueue");
    }
    queue
        .sub(&mut output, SvmQueueConditionalWait::Nowait)
        .expect("dequeue");
    assert_eq!(value(&output), 1);
    queue.add(&bytes(4), true).expect("wrapped add");

    for item in [2, 3, 4] {
        queue
            .sub(&mut output, SvmQueueConditionalWait::Nowait)
            .expect("dequeue");
        assert_eq!(value(&output), item);
    }
    unsafe { queue.destroy().expect("queue destroy") };
}

#[test]
fn queue_raw_add_notifies_a_waiting_consumer() {
    let segment = segment("hammer-svm-queue-raw");
    let queue = queue(&segment, 2);
    let consumer = unsafe { SvmQueue::attach(&segment, QUEUE_OFFSET) }.expect("consumer attach");

    let worker = thread::spawn(move || {
        let mut output = [0; size_of::<u64>()];
        consumer
            .sub(&mut output, SvmQueueConditionalWait::Wait)
            .map(|()| value(&output))
    });
    thread::sleep(Duration::from_millis(20));
    {
        let mut lock = queue.lock().expect("queue lock");
        unsafe { lock.add_raw(&bytes(123)).expect("raw add") };
    }

    assert_eq!(
        worker.join().expect("consumer worker").expect("dequeue"),
        123
    );
    unsafe { queue.destroy().expect("queue destroy") };
}

#[test]
fn queue_eventfd_signal_matches_vpp_producer_notification() {
    let segment = segment("hammer-svm-queue-eventfd");
    let mut queue = queue(&segment, 2);
    let event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    assert!(event_fd >= 0, "eventfd creation failed");
    let reader_fd = unsafe { libc::dup(event_fd) };
    assert!(reader_fd >= 0, "eventfd duplication failed");
    let reader = unsafe { OwnedFd::from_raw_fd(reader_fd) };
    queue.set_producer_event_fd(unsafe { OwnedFd::from_raw_fd(event_fd) });

    queue.add(&bytes(55), true).expect("eventfd add");
    let mut signal_value = 0_u64;
    let read = unsafe {
        libc::read(
            reader.as_raw_fd(),
            (&mut signal_value as *mut u64).cast(),
            size_of::<u64>(),
        )
    };
    assert_eq!(read, size_of::<u64>() as isize);
    assert_eq!(signal_value, 1);
    let mut output = [0; size_of::<u64>()];
    queue
        .sub(&mut output, SvmQueueConditionalWait::Nowait)
        .expect("dequeue");
    assert_eq!(value(&output), 55);
    unsafe { queue.destroy().expect("queue destroy") };
}

#[test]
fn queue_attach_uses_the_shared_layout_without_an_allocator() {
    let segment = segment("hammer-svm-queue-attach");
    let queue = queue(&segment, 2);
    segment.publish_ready();
    let descriptor = segment.fd().expect("segment descriptor");
    let client = SsvmPrivate::client_init_memfd(descriptor).expect("client mapping");
    let attached = unsafe { SvmQueue::attach(&client, QUEUE_OFFSET) }.expect("queue attach");

    attached
        .add(&bytes(7), false)
        .expect("attached producer add");
    let mut output = [0; size_of::<u64>()];
    queue
        .sub(&mut output, SvmQueueConditionalWait::Nowait)
        .expect("dequeue");
    assert_eq!(value(&output), 7);

    drop(attached);
    drop(client);
    unsafe { queue.destroy().expect("queue destroy") };
}
