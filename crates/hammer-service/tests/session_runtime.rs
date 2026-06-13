use hammer_service::session::{SessionId, SessionReadyQueue};

#[test]
fn session_ready_queue_dedupes_session_ids() {
    let mut ready = SessionReadyQueue::new();
    let first = SessionId::new(7);
    let second = SessionId::new(8);

    ready.mark_ready(first);
    ready.mark_ready(first);
    ready.mark_ready(second);

    assert_eq!(ready.take_ready_sessions(), vec![first, second]);
    assert!(ready.take_ready_sessions().is_empty());
}
