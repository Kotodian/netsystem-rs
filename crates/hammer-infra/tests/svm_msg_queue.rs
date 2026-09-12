//! Behavior tests for the VPP-style shared-memory multi-ring message queue.

#![cfg(target_os = "linux")]

use std::time::Duration;

use hammer_infra::svm::msg_queue::{
    SvmMsgQ, SvmMsgQConfig, SvmMsgQDescriptor, SvmMsgQError, SvmMsgQRingConfig,
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

fn write_u32(queue: &SvmMsgQ<'_>, message: SvmMsgQDescriptor, value: u32) {
    let data = unsafe { queue.msg_data(message).expect("message data") };
    unsafe { data.cast::<u32>().write_unaligned(value) };
}

fn read_u32(queue: &SvmMsgQ<'_>, message: SvmMsgQDescriptor) -> u32 {
    let data = unsafe { queue.msg_data(message).expect("message data") };
    unsafe { data.cast::<u32>().read_unaligned() }
}

#[test]
fn message_queue_matches_vpp_session_test_mq_basic() {
    let segment = segment("hammer-svm-msg-basic");
    let mut first_data = vec![0_u8; 8 * 8];
    let mut second_data = vec![0_u8; 8 * 16];
    let mut ring_cfgs = [
        SvmMsgQRingConfig::new(8, 8, &mut first_data),
        SvmMsgQRingConfig::new(8, 16, &mut second_data),
    ];
    let config = SvmMsgQConfig {
        consumer_pid: -1,
        q_nitems: 16,
        ring_cfgs: &mut ring_cfgs,
    };
    let queue = unsafe { SvmMsgQ::init_at(&segment, QUEUE_OFFSET, &config) }
        .expect("message queue initialization");

    let msg1 = queue.alloc_msg(8);
    assert!(!msg1.is_invalid());
    assert_eq!(queue.ring_size(0).expect("ring 0 size"), 1);
    assert_eq!(msg1.ring_index(), 0);
    assert_eq!(msg1.elt_index(), 0);

    let msg2 = queue.alloc_msg(15);
    assert!(!msg2.is_invalid());
    assert_eq!(queue.ring_size(1).expect("ring 1 size"), 1);
    assert_eq!(msg2.ring_index(), 1);
    assert_eq!(msg2.elt_index(), 0);

    queue.free_msg(msg1).expect("free first message");
    assert_eq!(queue.ring_size(0).expect("ring 0 size"), 0);

    let mut messages = [SvmMsgQDescriptor::INVALID; 12];
    for (index, message) in messages.iter_mut().enumerate() {
        *message = queue.alloc_msg(7);
        assert!(!message.is_invalid());
        write_u32(&queue, *message, index as u32);
    }

    assert_eq!(queue.ring_size(0).expect("ring 0 size"), 8);
    assert_eq!(queue.ring_size(1).expect("ring 1 size"), 5);

    write_u32(&queue, msg2, 123);
    queue.add(msg2, true).expect("publish message 2");
    for message in messages.iter().copied() {
        queue.add(message, true).expect("publish message");
    }

    let received = queue
        .sub(SvmQueueConditionalWait::Nowait)
        .expect("dequeue message 2");
    assert_eq!(received.ring_index(), 1);
    assert_eq!(received.elt_index(), 0);
    assert_eq!(read_u32(&queue, received), 123);
    queue.free_msg(received).expect("free message 2");

    for (index, message) in messages.iter().enumerate() {
        let received = queue
            .sub(SvmQueueConditionalWait::Nowait)
            .expect("dequeue message");
        assert_eq!(received, *message);
        if index < 8 {
            assert_eq!(received.ring_index(), 0);
            assert_eq!(received.elt_index(), (index as u32 + 1) % 8);
        } else {
            assert_eq!(received.ring_index(), 1);
            assert_eq!(received.elt_index(), (index as u32 - 8) + 1);
        }
        assert_eq!(read_u32(&queue, received), index as u32);
        queue.free_msg(received).expect("free message");
    }

    assert_eq!(queue.ring_size(0).expect("ring 0 size"), 0);
    assert_eq!(queue.ring_size(1).expect("ring 1 size"), 0);
    assert!(queue.is_empty());
}

#[test]
fn message_queue_nowait_full_keeps_allocated_descriptor() {
    let segment = segment("hammer-svm-msg-full");
    let mut data = vec![0_u8; 2 * 8];
    let mut ring_cfgs = [SvmMsgQRingConfig::new(2, 8, &mut data)];
    let config = SvmMsgQConfig {
        consumer_pid: -1,
        q_nitems: 1,
        ring_cfgs: &mut ring_cfgs,
    };
    let queue = unsafe { SvmMsgQ::init_at(&segment, QUEUE_OFFSET, &config) }
        .expect("message queue initialization");

    let first = queue.alloc_msg(8);
    let second = queue.alloc_msg(8);
    queue.add(first, true).expect("publish first message");
    assert!(matches!(
        queue.add(second, true),
        Err(SvmMsgQError::QueueFull)
    ));

    let received = queue
        .sub(SvmQueueConditionalWait::Nowait)
        .expect("dequeue first message");
    assert_eq!(received, first);
    queue.free_msg(received).expect("free first message");

    queue
        .add(second, true)
        .expect("publish retained descriptor");
    let received = queue
        .sub(SvmQueueConditionalWait::Nowait)
        .expect("dequeue second message");
    assert_eq!(received, second);
    queue.free_msg(received).expect("free second message");
    assert_eq!(queue.ring_size(0).expect("ring size"), 0);
}

#[test]
fn message_queue_timed_sub_reports_vpp_timeout() {
    let segment = segment("hammer-svm-msg-timeout");
    let mut data = vec![0_u8; 8];
    let mut ring_cfgs = [SvmMsgQRingConfig::new(1, 8, &mut data)];
    let config = SvmMsgQConfig {
        consumer_pid: -1,
        q_nitems: 1,
        ring_cfgs: &mut ring_cfgs,
    };
    let queue = unsafe { SvmMsgQ::init_at(&segment, QUEUE_OFFSET, &config) }
        .expect("message queue initialization");

    assert!(matches!(
        queue.sub(SvmQueueConditionalWait::TimedWait(Duration::from_millis(
            10
        ))),
        Err(SvmMsgQError::Timeout)
    ));
}
