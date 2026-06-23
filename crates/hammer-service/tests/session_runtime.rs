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

#[test]
fn session_ready_queue_preserves_fifo_order_across_multiple_sessions() {
    let mut ready = SessionReadyQueue::new();
    let first = SessionId::new(11);
    let second = SessionId::new(12);
    let third = SessionId::new(13);

    ready.mark_ready(first);
    ready.mark_ready(second);
    ready.mark_ready(third);

    assert_eq!(ready.take_ready_sessions(), vec![first, second, third]);
}
