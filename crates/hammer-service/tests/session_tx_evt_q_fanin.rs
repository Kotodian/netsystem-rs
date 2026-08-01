//! Application Rx MQ isolation at the Session Message Queue boundary.

use std::sync::Arc;

use hammer_runtime::app::{SessionEvt, SessionEvtType, SessionMsgQueue};

#[test]
fn one_application_mq_full_does_not_block_another_application_mq() {
    let first: Arc<SessionMsgQueue> = Arc::new(SessionMsgQueue::with_cfg(8, 2).expect("first MQ"));
    let second: Arc<SessionMsgQueue> =
        Arc::new(SessionMsgQueue::with_cfg(8, 2).expect("second MQ"));

    first
        .enqueue_io(SessionEvt::io(1, SessionEvtType::TxEnq))
        .expect("first Application event");
    first
        .enqueue_io(SessionEvt::io(2, SessionEvtType::TxEnq))
        .expect("second first-Application event");
    assert!(
        first
            .enqueue_io(SessionEvt::io(3, SessionEvtType::TxEnq))
            .is_err()
    );

    second
        .enqueue_io(SessionEvt::io(9, SessionEvtType::TxEnq))
        .expect("second Application remains writable");
    assert_eq!(
        second
            .dequeue()
            .expect("second Application event")
            .session_index(),
        9
    );
    assert_eq!(
        first
            .dequeue()
            .expect("first Application event")
            .session_index(),
        1
    );
}
