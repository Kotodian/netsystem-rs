//! Smoke test for local and external per-Application Session Message Queues.
//!
//! Run with:
//!
//! ```text
//! cargo run -p hammer --example application_mq_isolation
//! ```

use std::io;
use std::sync::Arc;

use hammer_infra::segment::Segment;
use hammer_runtime::app::{SessionEvt, SessionEvtType, SessionMsgQueue, SessionMsgQueueError};

const Q_NITEMS: u32 = 8;
const RING_NITEMS: u32 = 2;
const SEGMENT_BYTES: usize = 4096;

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error(transparent)]
    Queue(#[from] SessionMsgQueueError),
    #[error("failed to create the external Application MQ segment")]
    ExternalSegment {
        #[source]
        source: io::Error,
    },
    #[error("{application} Application MQ segment cannot hold its queue")]
    SegmentExhausted { application: &'static str },
}

fn main() -> Result<(), ExampleError> {
    let local_application_mq = create_local_application_mq()?;
    let external_application_mq = create_external_application_mq()?;

    let local_first = SessionEvt::io(1, SessionEvtType::TxEnq);
    let local_second = SessionEvt::io(2, SessionEvtType::TxEnq);
    let local_blocked = SessionEvt::io(3, SessionEvtType::TxEnq);
    local_application_mq.enqueue_io(local_first)?;
    local_application_mq.enqueue_io(local_second)?;
    expect_io_full(&local_application_mq, local_blocked, "local");

    // Local Application MQ exhaustion does not affect the external MQ.
    let external_first = SessionEvt::io(9, SessionEvtType::TxEnq);
    let external_second = SessionEvt::io(10, SessionEvtType::TxEnq);
    let external_blocked = SessionEvt::io(11, SessionEvtType::TxEnq);
    external_application_mq.enqueue_io(external_first)?;
    external_application_mq.enqueue_io(external_second)?;
    expect_io_full(&external_application_mq, external_blocked, "external");

    assert_eq!(
        local_application_mq.dequeue(),
        Some(local_first),
        "local MQ returned an event from another Application"
    );
    assert_eq!(
        local_application_mq.dequeue(),
        Some(local_second),
        "local MQ lost its own event ordering"
    );
    assert_eq!(
        external_application_mq.dequeue(),
        Some(external_first),
        "external MQ returned an event from another Application"
    );
    assert_eq!(
        external_application_mq.dequeue(),
        Some(external_second),
        "external MQ lost its own event ordering"
    );

    // External Application MQ exhaustion does not affect the local MQ.
    let local_after_external_full = SessionEvt::io(4, SessionEvtType::TxEnq);
    local_application_mq.enqueue_io(local_after_external_full)?;
    assert_eq!(
        local_application_mq.dequeue(),
        Some(local_after_external_full)
    );
    assert_eq!(local_application_mq.dequeue(), None);
    assert_eq!(external_application_mq.dequeue(), None);

    println!("local Application MQ: PASS");
    println!("external Application MQ: PASS");
    println!("local/external Application MQ isolation: PASS");
    Ok(())
}

fn create_local_application_mq() -> Result<Arc<SessionMsgQueue>, ExampleError> {
    let segment = Segment::local(SEGMENT_BYTES);
    assert!(
        segment.shared_fd().is_none(),
        "local Application MQ unexpectedly exposes a shared descriptor"
    );
    let offset = segment
        .alloc(queue_bytes()?, 64)
        .ok_or(ExampleError::SegmentExhausted {
            application: "local",
        })?;
    // SAFETY: `offset` is allocated from `segment` using the exact queue
    // layout, and this call is the sole initializer for that allocation.
    let queue =
        unsafe { SessionMsgQueue::init_at_with_signal(segment, offset, Q_NITEMS, RING_NITEMS) }?;
    Ok(Arc::new(queue))
}

fn create_external_application_mq() -> Result<Arc<SessionMsgQueue>, ExampleError> {
    let segment_name = format!("hammer-mq-{}", std::process::id());
    let segment = Segment::shared(&segment_name, SEGMENT_BYTES)
        .map_err(|source| ExampleError::ExternalSegment { source })?;
    assert!(
        segment.shared_fd().is_some(),
        "external Application MQ does not expose a shared descriptor"
    );
    let offset = segment
        .alloc(queue_bytes()?, 64)
        .ok_or(ExampleError::SegmentExhausted {
            application: "external",
        })?;
    // SAFETY: `offset` is allocated from `segment` using the exact queue
    // layout, and this call is the sole initializer for that allocation.
    let queue =
        unsafe { SessionMsgQueue::init_at_with_signal(segment, offset, Q_NITEMS, RING_NITEMS) }?;
    Ok(Arc::new(queue))
}

fn queue_bytes() -> Result<usize, SessionMsgQueueError> {
    SessionMsgQueue::layout_bytes(Q_NITEMS, RING_NITEMS)
}

fn expect_io_full(queue: &SessionMsgQueue, event: SessionEvt, application: &'static str) {
    match queue.enqueue_io(event) {
        Err(SessionMsgQueueError::Full(rejected)) if rejected == event => {}
        other => panic!("{application} Application MQ full check failed: {other:?}"),
    }
}
