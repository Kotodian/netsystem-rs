//! VPP message queue lifecycle tests.

#![cfg(target_os = "linux")]

use std::time::Duration;

use hammer_infra::svm::msg_queue::{
    SvmMsgQ, SvmMsgQConfig, SvmMsgQError, SvmMsgQRingConfig, SvmMsgQWaitType,
};
use hammer_infra::svm::queue::SvmQueueConditionalWait;
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

#[test]
fn message_queue_keeps_descriptor_and_ring_lifecycles_separate() {
    let segment = segment("hammer-svm-msg-order");
    let mut first_data = vec![0_u8; 2 * 8];
    let mut second_data = vec![0_u8; 2 * 16];
    let mut ring_cfgs = [
        SvmMsgQRingConfig::new(2, 8, &mut first_data),
        SvmMsgQRingConfig::new(2, 16, &mut second_data),
    ];
    let config = SvmMsgQConfig {
        consumer_pid: std::process::id() as i32,
        q_nitems: 4,
        ring_cfgs: &mut ring_cfgs,
    };
    let queue = unsafe { SvmMsgQ::init_at(&segment, QUEUE_OFFSET, &config) }
        .expect("message queue initialization");

    let first = queue.alloc_msg_w_ring(0).expect("first ring allocation");
    unsafe {
        std::ptr::copy_nonoverlapping(
            123_u64.to_ne_bytes().as_ptr(),
            queue.msg_data(first).expect("first slot"),
            8,
        )
    };
    queue.add(first, true).expect("first publish");

    let second = queue.alloc_msg_w_ring(1).expect("second ring allocation");
    unsafe {
        std::ptr::copy_nonoverlapping(
            456_u64.to_ne_bytes().as_ptr(),
            queue.msg_data(second).expect("second slot"),
            8,
        )
    };
    queue.add(second, true).expect("second publish");

    let received_first = queue
        .sub(SvmQueueConditionalWait::Nowait)
        .expect("first dequeue");
    assert_eq!(received_first.parts().ring_index, 0);
    let mut first_bytes = [0_u8; 8];
    unsafe {
        std::ptr::copy_nonoverlapping(
            queue.msg_data(received_first).unwrap(),
            first_bytes.as_mut_ptr(),
            8,
        )
    };
    assert_eq!(u64::from_ne_bytes(first_bytes), 123);
    queue.free_msg(received_first).expect("first release");

    let received_second = queue
        .sub(SvmQueueConditionalWait::Nowait)
        .expect("second dequeue");
    assert_eq!(received_second.parts().ring_index, 1);
    let mut second_bytes = [0_u8; 8];
    unsafe {
        std::ptr::copy_nonoverlapping(
            queue.msg_data(received_second).unwrap(),
            second_bytes.as_mut_ptr(),
            8,
        )
    };
    assert_eq!(u64::from_ne_bytes(second_bytes), 456);
    queue.free_msg(received_second).expect("second release");
    assert!(queue.is_empty());
}

#[test]
fn message_queue_reports_ring_full_and_descriptor_queue_full_independently() {
    let segment = segment("hammer-svm-msg-full");
    let mut data = vec![0_u8; 2 * 8];
    let mut ring_cfgs = [SvmMsgQRingConfig::new(2, 8, &mut data)];
    let config = SvmMsgQConfig {
        consumer_pid: 0,
        q_nitems: 1,
        ring_cfgs: &mut ring_cfgs,
    };
    let queue = unsafe { SvmMsgQ::init_at(&segment, QUEUE_OFFSET, &config) }
        .expect("message queue initialization");

    let first = queue.alloc_msg_w_ring(0).expect("first allocation");
    let second = queue.alloc_msg_w_ring(0).expect("second allocation");
    assert!(matches!(
        queue.alloc_msg_w_ring(0),
        Err(SvmMsgQError::RingFull { ring: 0 })
    ));
    queue.add(first, true).expect("first publish");
    assert!(matches!(
        queue.add(second, true),
        Err(SvmMsgQError::QueueFull)
    ));

    let received = queue.sub(SvmQueueConditionalWait::Nowait).expect("dequeue");
    queue.free_msg(received).expect("release");
    queue
        .add(second, true)
        .expect("publish after descriptor space");
}

#[test]
fn message_queue_waits_on_descriptor_empty_predicate() {
    let segment = segment("hammer-svm-msg-wait");
    let mut data = vec![0_u8; 8];
    let mut ring_cfgs = [SvmMsgQRingConfig::new(1, 8, &mut data)];
    let config = SvmMsgQConfig {
        consumer_pid: 0,
        q_nitems: 1,
        ring_cfgs: &mut ring_cfgs,
    };
    let queue = unsafe { SvmMsgQ::init_at(&segment, QUEUE_OFFSET, &config) }
        .expect("message queue initialization");
    assert!(matches!(
        queue.timedwait(SvmMsgQWaitType::Empty, Duration::from_millis(10)),
        Ok(_)
    ));
}
