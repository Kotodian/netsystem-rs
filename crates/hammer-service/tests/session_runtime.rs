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

#[test]
fn session_queue_node_does_not_clear_input_frame() {
    let source = include_str!("../src/session/node.rs");

    assert!(
        !source.contains("frame.clear()"),
        "SessionQueueNode is a polling input driver and must not consume input frames"
    );
}

#[test]
fn tun_boundary_does_not_use_public_copy_packet_api() {
    let source = include_str!("../src/tun/mod.rs");

    assert!(
        !source.contains("copy_packet"),
        "TUN boundary must read buffer chains locally instead of calling copy_packet"
    );
}
