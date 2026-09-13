//! Public configuration checks for the owner-initialized shared queue.

#![cfg(target_os = "linux")]

use hammer_infra::svm::queue::{SvmQueue, SvmQueueConfig, SvmQueueError};

#[test]
fn queue_layout_accounts_for_inline_data() {
    let config = SvmQueueConfig {
        nels: 4,
        elsize: 8,
        consumer_pid: -1,
    };
    let size = SvmQueue::size_to_alloc(&config).expect("queue layout");
    assert!(size >= 4 * 8);
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
