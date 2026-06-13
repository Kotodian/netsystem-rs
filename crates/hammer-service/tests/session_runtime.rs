use hammer_service::session::{AppSessionId, AppSessionReadyQueue};

#[test]
fn app_session_ready_queue_dedupes_session_ids() {
    let mut ready = AppSessionReadyQueue::new();
    let first = AppSessionId::new(7);
    let second = AppSessionId::new(8);

    ready.mark_ready(first);
    ready.mark_ready(first);
    ready.mark_ready(second);

    assert_eq!(ready.take_ready_sessions(), vec![first, second]);
    assert!(ready.take_ready_sessions().is_empty());
}
