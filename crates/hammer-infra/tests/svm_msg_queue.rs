//! Behavior tests for the VPP-style shared-memory multi-ring message queue.

#![cfg(target_os = "linux")]

use std::process::Command;
use std::ptr::NonNull;
use std::sync::OnceLock;
use std::time::Duration;

use byte_unit::Byte;
use hammer_infra::mem::MainHeapConfig;
use hammer_infra::svm::msg_queue::{
    SvmMsgQ, SvmMsgQConfig, SvmMsgQDescriptor, SvmMsgQError, SvmMsgQRingConfig,
};
use hammer_infra::svm::queue::SvmQueueConditionalWait;
use hammer_infra::svm::ssvm::{SSVM_PAYLOAD_OFFSET, SsvmConfig, SsvmPrivate, SsvmSegmentBackend};

const SEGMENT_BYTES: usize = 1 << 16;
const QUEUE_OFFSET: u64 = SSVM_PAYLOAD_OFFSET + 4096;
const PROCESS_MODE: &str = "HAMMER_SVM_MSG_QUEUE_PROCESS_MODE";

fn initialize_main_heap() {
    static MAIN_HEAP: OnceLock<()> = OnceLock::new();
    MAIN_HEAP.get_or_init(|| {
        MainHeapConfig {
            size: Byte::from_u64(256 << 20),
            page_size: hammer_infra::PageSize::Default,
            default_hugepage_size: None,
        }
        .initialize()
        .expect("initialize process Main Heap");
    });
}

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

fn run_test_process(mode: &str, test_name: &str) {
    if let Ok(child_mode) = std::env::var(PROCESS_MODE) {
        initialize_main_heap();
        match child_mode.as_str() {
            "basic" => message_queue_matches_vpp_session_test_mq_basic_case(),
            "queue-full" => message_queue_nowait_full_keeps_allocated_descriptor_case(),
            "timeout" => message_queue_timed_sub_reports_vpp_timeout_case(),
            _ => panic!("unknown SVM message queue process mode {child_mode}"),
        }
        unsafe { libc::_exit(0) }
    }

    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .env(PROCESS_MODE, mode)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .output()
        .expect("spawn SVM message queue test process");
    assert!(
        output.status.success(),
        "SVM message queue process {mode} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn message_queue_matches_vpp_session_test_mq_basic() {
    run_test_process("basic", "message_queue_matches_vpp_session_test_mq_basic");
}

fn message_queue_matches_vpp_session_test_mq_basic_case() {
    let segment = segment("hammer-svm-msg-basic");
    let ring_cfgs = [SvmMsgQRingConfig::new(8, 8), SvmMsgQRingConfig::new(8, 16)];
    let config = SvmMsgQConfig {
        consumer_pid: -1,
        q_nitems: 16,
        rings: &ring_cfgs,
    };
    let base = NonNull::new(unsafe { segment.base().add(QUEUE_OFFSET as usize) })
        .expect("message queue base");
    let queue = unsafe { SvmMsgQ::init(base, &config) }.expect("message queue initialization");
    let attached = unsafe { SvmMsgQ::attach(base) }.expect("message queue attach");
    assert!(attached.is_empty());

    let mut producer = queue
        .producer(SvmQueueConditionalWait::Nowait)
        .expect("producer lock");
    let msg1 = producer.alloc_msg(8).expect("message 1 allocation");
    assert!(!msg1.is_invalid());
    assert_eq!(queue.ring_size(0).expect("ring 0 size"), 1);
    assert_eq!(msg1.ring_index(), 0);
    assert_eq!(msg1.elt_index(), 0);

    let msg2 = producer.alloc_msg(15).expect("message 2 allocation");
    assert!(!msg2.is_invalid());
    assert_eq!(queue.ring_size(1).expect("ring 1 size"), 1);
    assert_eq!(msg2.ring_index(), 1);
    assert_eq!(msg2.elt_index(), 0);
    drop(producer);

    queue.free_msg(msg1).expect("free first message");
    assert_eq!(queue.ring_size(0).expect("ring 0 size"), 0);

    let mut messages = [SvmMsgQDescriptor::INVALID; 12];
    let mut producer = queue
        .producer(SvmQueueConditionalWait::Nowait)
        .expect("producer lock");
    for (index, message) in messages.iter_mut().enumerate() {
        *message = producer.alloc_msg(7).expect("message allocation");
        assert!(!message.is_invalid());
        if message.ring_index() == 0 {
            let mut value = [0_u8; 8];
            value[..4].copy_from_slice(&(index as u32).to_ne_bytes());
            producer.write(*message, &value).expect("ring 0 write");
        } else {
            let mut value = [0_u8; 16];
            value[..4].copy_from_slice(&(index as u32).to_ne_bytes());
            producer.write(*message, &value).expect("ring 1 write");
        }
    }

    assert_eq!(queue.ring_size(0).expect("ring 0 size"), 8);
    assert_eq!(queue.ring_size(1).expect("ring 1 size"), 5);

    let mut value = [0_u8; 16];
    value[..4].copy_from_slice(&123_u32.to_ne_bytes());
    producer.write(msg2, &value).expect("ring 1 write");
    producer.add(msg2).expect("publish message 2");
    for message in messages.iter().copied() {
        producer.add(message).expect("publish message");
    }
    drop(producer);

    let received = queue
        .sub(SvmQueueConditionalWait::Nowait)
        .expect("dequeue message 2");
    assert_eq!(received.ring_index(), 1);
    assert_eq!(received.elt_index(), 0);
    assert_eq!(
        queue.read::<[u8; 16]>(received).expect("read message")[..4],
        123_u32.to_ne_bytes()
    );
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
        if received.ring_index() == 0 {
            assert_eq!(
                queue.read::<[u8; 8]>(received).expect("read message")[..4],
                (index as u32).to_ne_bytes()
            );
        } else {
            assert_eq!(
                queue.read::<[u8; 16]>(received).expect("read message")[..4],
                (index as u32).to_ne_bytes()
            );
        }
        queue.free_msg(received).expect("free message");
    }

    assert_eq!(queue.ring_size(0).expect("ring 0 size"), 0);
    assert_eq!(queue.ring_size(1).expect("ring 1 size"), 0);
    assert!(queue.is_empty());
}

#[test]
fn message_queue_nowait_full_keeps_allocated_descriptor() {
    run_test_process(
        "queue-full",
        "message_queue_nowait_full_keeps_allocated_descriptor",
    );
}

fn message_queue_nowait_full_keeps_allocated_descriptor_case() {
    let segment = segment("hammer-svm-msg-full");
    let ring_cfgs = [SvmMsgQRingConfig::new(2, 8)];
    let config = SvmMsgQConfig {
        consumer_pid: -1,
        q_nitems: 1,
        rings: &ring_cfgs,
    };
    let base = NonNull::new(unsafe { segment.base().add(QUEUE_OFFSET as usize) })
        .expect("message queue base");
    let queue = unsafe { SvmMsgQ::init(base, &config) }.expect("message queue initialization");

    let mut producer = queue
        .producer(SvmQueueConditionalWait::Nowait)
        .expect("producer lock");
    let first = producer.alloc_msg(8).expect("first allocation");
    let second = producer.alloc_msg(8).expect("second allocation");
    producer.add(first).expect("publish first message");
    assert!(matches!(producer.add(second), Err(SvmMsgQError::QueueFull)));
    drop(producer);

    let received = queue
        .sub(SvmQueueConditionalWait::Nowait)
        .expect("dequeue first message");
    assert_eq!(received, first);
    queue.free_msg(received).expect("free first message");

    queue
        .producer(SvmQueueConditionalWait::Nowait)
        .expect("producer lock")
        .add(second)
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
    run_test_process("timeout", "message_queue_timed_sub_reports_vpp_timeout");
}

fn message_queue_timed_sub_reports_vpp_timeout_case() {
    let segment = segment("hammer-svm-msg-timeout");
    let ring_cfgs = [SvmMsgQRingConfig::new(1, 8)];
    let config = SvmMsgQConfig {
        consumer_pid: -1,
        q_nitems: 1,
        rings: &ring_cfgs,
    };
    let base = NonNull::new(unsafe { segment.base().add(QUEUE_OFFSET as usize) })
        .expect("message queue base");
    let queue = unsafe { SvmMsgQ::init(base, &config) }.expect("message queue initialization");

    assert!(matches!(
        queue.sub(SvmQueueConditionalWait::TimedWait(Duration::from_millis(
            10
        ))),
        Err(SvmMsgQError::Timeout)
    ));
}
