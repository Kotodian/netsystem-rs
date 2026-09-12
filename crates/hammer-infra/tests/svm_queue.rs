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

fn queue(segment: &SsvmPrivate, capacity: u32) -> SvmQueue<u64> {
    unsafe {
        SvmQueue::init_at(
            segment,
            QUEUE_OFFSET,
            &SvmQueueConfig { capacity },
            std::process::id() as i32,
        )
    }
    .expect("queue initialization")
}

fn destroy(queue: SvmQueue<u64>) {
    unsafe { queue.destroy().expect("queue destroy") };
}

#[test]
fn queue_preserves_fifo_order_and_reports_vpp_metadata() {
    let segment = segment("hammer-svm-queue-order");
    let queue = queue(&segment, 4);

    assert_eq!(queue.capacity(), 4);
    assert_eq!(queue.element_size(), size_of::<u64>());
    assert_eq!(queue.consumer_pid(), std::process::id() as i32);

    queue.add(10, false).expect("first add");
    queue.add(20, false).expect("second add");
    assert_eq!(queue.len().expect("queue length"), 2);
    assert_eq!(queue.sub(SvmQueueConditionalWait::Nowait).unwrap(), 10);
    assert_eq!(queue.sub(SvmQueueConditionalWait::Nowait).unwrap(), 20);
    assert!(queue.is_empty().expect("empty queue"));

    destroy(queue);
}

#[test]
fn queue_add2_is_atomic_when_two_slots_are_unavailable() {
    let segment = segment("hammer-svm-queue-add2");
    let queue = queue(&segment, 2);

    queue.add2(1, 2, false).expect("pair add");
    assert!(queue.is_full().expect("full queue"));
    assert!(matches!(
        queue.add2(3, 4, true),
        Err(SvmQueueError::QueueFull)
    ));
    assert_eq!(queue.sub2().unwrap(), Some(1));
    assert_eq!(queue.sub2().unwrap(), Some(2));
    assert_eq!(queue.sub2().unwrap(), None);

    destroy(queue);
}

#[test]
fn queue_wait_wakes_after_producer_adds_an_element() {
    let segment = segment("hammer-svm-queue-wait");
    let queue = queue(&segment, 2);
    let consumer =
        unsafe { SvmQueue::<u64>::attach(&segment, QUEUE_OFFSET) }.expect("consumer attach");
    let producer =
        unsafe { SvmQueue::<u64>::attach(&segment, QUEUE_OFFSET) }.expect("producer attach");

    let worker = thread::spawn(move || consumer.sub(SvmQueueConditionalWait::Wait));
    thread::sleep(Duration::from_millis(20));
    producer.add(99, false).expect("producer add");
    assert_eq!(
        worker
            .join()
            .expect("consumer worker")
            .expect("consumer dequeue"),
        99
    );

    drop(producer);
    destroy(queue);
}

#[test]
fn queue_timedwait_returns_timeout_and_nowait_distinguishes_empty() {
    let segment = segment("hammer-svm-queue-timeout");
    let queue = queue(&segment, 1);

    assert!(matches!(
        queue.sub(SvmQueueConditionalWait::Nowait),
        Err(SvmQueueError::Empty)
    ));
    assert!(matches!(
        queue.sub(SvmQueueConditionalWait::TimedWait(Duration::from_millis(
            20
        ))),
        Err(SvmQueueError::Timeout)
    ));

    destroy(queue);
}

#[test]
fn queue_wraps_a_non_power_of_two_ring() {
    let segment = segment("hammer-svm-queue-wrap");
    let queue = queue(&segment, 3);

    queue.add(1, true).expect("first add");
    queue.add(2, true).expect("second add");
    queue.add(3, true).expect("third add");
    assert_eq!(queue.sub(SvmQueueConditionalWait::Nowait).unwrap(), 1);
    queue.add(4, true).expect("wrapped add");

    assert_eq!(queue.sub(SvmQueueConditionalWait::Nowait).unwrap(), 2);
    assert_eq!(queue.sub(SvmQueueConditionalWait::Nowait).unwrap(), 3);
    assert_eq!(queue.sub(SvmQueueConditionalWait::Nowait).unwrap(), 4);
    destroy(queue);
}

#[test]
fn queue_raw_add_notifies_a_waiting_consumer() {
    let segment = segment("hammer-svm-queue-raw");
    let queue = queue(&segment, 2);
    let consumer =
        unsafe { SvmQueue::<u64>::attach(&segment, QUEUE_OFFSET) }.expect("consumer attach");

    let worker = thread::spawn(move || consumer.sub(SvmQueueConditionalWait::Wait));
    thread::sleep(Duration::from_millis(20));
    {
        let mut lock = queue.lock().expect("queue lock");
        unsafe { lock.add_raw(123).expect("raw add") };
    }

    assert_eq!(
        worker
            .join()
            .expect("consumer worker")
            .expect("consumer dequeue"),
        123
    );
    destroy(queue);
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

    queue.add(55, true).expect("eventfd add");
    let mut value = 0_u64;
    let read = unsafe {
        libc::read(
            reader.as_raw_fd(),
            (&mut value as *mut u64).cast(),
            size_of::<u64>(),
        )
    };
    assert_eq!(read, size_of::<u64>() as isize);
    assert_eq!(value, 1);
    assert_eq!(queue.sub(SvmQueueConditionalWait::Nowait).unwrap(), 55);
    destroy(queue);
}

#[test]
fn queue_attach_uses_the_shared_layout_without_an_allocator() {
    let segment = segment("hammer-svm-queue-attach");
    let queue = queue(&segment, 2);
    segment.publish_ready();
    let descriptor = segment.fd().expect("segment descriptor");
    let client = SsvmPrivate::client_init_memfd(descriptor).expect("client mapping");
    let attached = unsafe { SvmQueue::<u64>::attach(&client, QUEUE_OFFSET) }.expect("queue attach");

    attached.add(7, false).expect("attached producer add");
    assert_eq!(queue.sub2().unwrap(), Some(7));

    drop(attached);
    drop(client);
    destroy(queue);
}
